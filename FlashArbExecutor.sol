// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "./Interfaces.sol";

contract FlashArbExecutor is
    IBalancerFlashLoanRecipient,
    IDODOCallee,
    IUniswapV3SwapCallback,
    IPancakeV3SwapCallback
{
    error UnauthorizedCaller();
    error UnauthorizedProvider();
    error UnauthorizedPool();
    error UnauthorizedCallback();
    error InvalidRoute();
    error InvalidLoanToken();
    error InvalidLoanAmount();
    error InvalidArrayLength();
    error InsufficientOutput(uint256 actual, uint256 required);
    error InsufficientProfit(uint256 actual, uint256 required);
    error SwapExecutionFailed();
    error LoanRepaymentFailed();
    error StaleTransaction();
    error InvalidExecutionState();
    error TokenContinuityFailed();
    error InvalidDexTypeForCallback();
    error InvalidBorrowedAsset();
    error InvalidHopCount();
    error UnsupportedPoolType();

    enum LoanProvider { BALANCER, DODO }
    enum ExecutionState { IDLE, LOAN_ACTIVE, SWAP_ACTIVE, COMPLETE }
    enum DexType { UNISWAP_V3, PANCAKE_V3, AERODROME_V2, ALIENBASE_V2 }

    struct RouteData {
        address[] pools;
        DexType[] dexTypes;
        bool[] zeroForOnes;
        uint256[] minOuts;
        uint256 amountIn;
    }

    address public immutable owner;
    IBalancerVault public immutable balancerVault;
    mapping(address => bool) public whitelistedDodoPools;

    // Per‑pool fee for V2‑style pools (in basis points, e.g. 30 = 0.3%)
    mapping(address => uint256) public poolFeeBps;

    // Mark pools that use a stable‑swap curve (not supported)
    mapping(address => bool) public stablePools;

    struct ExecutionContext {
        ExecutionState state;
        LoanProvider provider;
        address loanPool;
        address loanToken;
        uint256 loanAmount;
        uint256 minProfitWei;
        uint256 deadline;
        RouteData route;
        uint256 hop;
        uint256 initialBalance;
        bool hopPaid; // per‑hop callback guard
    }
    ExecutionContext private _ctx;

    // Events
    event ArbitrageExecuted(
        address indexed loanToken,
        uint256 loanAmount,
        uint256 profit,
        uint256 minProfit,
        bool success
    );
    event PoolFeeSet(address indexed pool, uint256 feeBps);
    event PoolStableSet(address indexed pool, bool isStable);
    event DodoPoolWhitelisted(address indexed pool, bool whitelisted);

    modifier onlyOwner() {
        if (msg.sender != owner) revert UnauthorizedCaller();
        _;
    }
    modifier onlyBalancerVault() {
        if (msg.sender != address(balancerVault)) revert UnauthorizedCallback();
        _;
    }
    modifier onlyWhitelistedDodo() {
        if (!whitelistedDodoPools[msg.sender]) revert UnauthorizedPool();
        _;
    }
    modifier onlyActiveLoan() {
        if (_ctx.state != ExecutionState.LOAN_ACTIVE) revert InvalidExecutionState();
        _;
    }
    modifier onlyActiveSwap() {
        if (_ctx.state != ExecutionState.SWAP_ACTIVE) revert InvalidExecutionState();
        _;
    }
    modifier onlyIdle() {
        if (_ctx.state != ExecutionState.IDLE) revert InvalidExecutionState();
        _;
    }

    constructor(address _balancerVault) {
        if (_balancerVault == address(0)) revert UnauthorizedProvider();
        owner = msg.sender;
        balancerVault = IBalancerVault(_balancerVault);
        _ctx.state = ExecutionState.IDLE;
        _ctx.hopPaid = false;
    }

    // ---- Owner admin functions ----
    function setPoolFee(address pool, uint256 feeBps) external onlyOwner {
        if (feeBps >= 10000) revert InvalidRoute();
        poolFeeBps[pool] = feeBps;
        emit PoolFeeSet(pool, feeBps);
    }

    function setPoolStable(address pool, bool isStable) external onlyOwner {
        stablePools[pool] = isStable;
        emit PoolStableSet(pool, isStable);
    }

    function whitelistDodoPool(address pool) external onlyOwner {
        whitelistedDodoPools[pool] = true;
        emit DodoPoolWhitelisted(pool, true);
    }

    function unwhitelistDodoPool(address pool) external onlyOwner {
        whitelistedDodoPools[pool] = false;
        emit DodoPoolWhitelisted(pool, false);
    }

    function withdrawToken(address token) external onlyOwner {
        uint256 balance = IERC20(token).balanceOf(address(this));
        if (balance > 0) {
            _safeTransfer(IERC20(token), owner, balance);
        }
    }

    function withdrawETH() external onlyOwner {
        (bool sent, ) = payable(owner).call{value: address(this).balance}("");
        require(sent, "ETH transfer failed");
    }

    // ---- Main entry ----
    function executeArbitrage(
        LoanProvider provider,
        address loanPool,
        address loanToken,
        uint256 loanAmount,
        RouteData calldata route,
        uint256 minProfitWei,
        uint256 deadline
    ) external onlyOwner onlyIdle {
        if (deadline < block.timestamp) revert StaleTransaction();

        if (provider == LoanProvider.BALANCER) {
            if (loanPool != address(balancerVault)) revert UnauthorizedPool();
        } else if (provider == LoanProvider.DODO) {
            if (!whitelistedDodoPools[loanPool]) revert UnauthorizedPool();
        } else {
            revert UnauthorizedProvider();
        }

        uint256 numHops = route.pools.length;
        if (numHops < 2 || numHops > 3) revert InvalidHopCount();
        if (route.dexTypes.length != numHops ||
            route.zeroForOnes.length != numHops ||
            route.minOuts.length != numHops) revert InvalidRoute();

        if (loanAmount != route.amountIn) revert InvalidLoanAmount();

        _ctx.state = ExecutionState.LOAN_ACTIVE;
        _ctx.provider = provider;
        _ctx.loanPool = loanPool;
        _ctx.loanToken = loanToken;
        _ctx.loanAmount = loanAmount;
        _ctx.minProfitWei = minProfitWei;
        _ctx.deadline = deadline;
        _ctx.route = route;
        _ctx.hop = 0;
        _ctx.initialBalance = IERC20(loanToken).balanceOf(address(this));
        _ctx.hopPaid = false;

        if (provider == LoanProvider.BALANCER) {
            address[] memory tokens = new address[](1);
            tokens[0] = loanToken;
            uint256[] memory amounts = new uint256[](1);
            amounts[0] = loanAmount;
            bytes memory userData = abi.encode(route, minProfitWei);
            balancerVault.flashLoan(address(this), tokens, amounts, userData);
        } else {
            IDODO pool = IDODO(loanPool);
            address base = pool._BASE_TOKEN_();
            address quote = pool._QUOTE_TOKEN_();
            uint256 baseAmount = (loanToken == base) ? loanAmount : 0;
            uint256 quoteAmount = (loanToken == quote) ? loanAmount : 0;
            if (baseAmount == 0 && quoteAmount == 0) revert InvalidLoanToken();
            bytes memory userData = abi.encode(route, minProfitWei, loanToken, loanAmount);
            pool.flashLoan(baseAmount, quoteAmount, address(this), userData);
        }
    }

    // ---- Balancer callback ----
    function receiveFlashLoan(
        address[] memory tokens,
        uint256[] memory amounts,
        uint256[] memory feeAmounts,
        bytes memory userData
    ) external override onlyBalancerVault onlyActiveLoan {
        if (tokens.length != 1 || amounts.length != 1 || feeAmounts.length != 1)
            revert InvalidArrayLength();
        if (tokens[0] != _ctx.loanToken) revert InvalidLoanToken();
        if (amounts[0] != _ctx.loanAmount) revert InvalidLoanAmount();

        (RouteData memory route, uint256 minProfit) = abi.decode(userData, (RouteData, uint256));
        if (minProfit != _ctx.minProfitWei) revert InvalidRoute();
        if (route.pools.length != _ctx.route.pools.length) revert InvalidRoute();
        for (uint i = 0; i < route.pools.length; i++) {
            if (route.pools[i] != _ctx.route.pools[i]) revert InvalidRoute();
        }

        _ctx.state = ExecutionState.SWAP_ACTIVE;
        _executeRoute();

        uint256 repay = amounts[0] + feeAmounts[0];
        _safeTransfer(IERC20(_ctx.loanToken), address(balancerVault), repay);

        _ctx.state = ExecutionState.COMPLETE;
        _finalize();
    }

    // ---- DODO callbacks ----
    function DVMFlashLoanCall(address sender, uint256 baseAmount, uint256 quoteAmount, bytes calldata data)
        external override onlyWhitelistedDodo onlyActiveLoan
    {
        _handleDodoCallback(sender, baseAmount, quoteAmount, data);
    }
    function DPPFlashLoanCall(address sender, uint256 baseAmount, uint256 quoteAmount, bytes calldata data)
        external override onlyWhitelistedDodo onlyActiveLoan
    {
        _handleDodoCallback(sender, baseAmount, quoteAmount, data);
    }
    function DSPFlashLoanCall(address sender, uint256 baseAmount, uint256 quoteAmount, bytes calldata data)
        external override onlyWhitelistedDodo onlyActiveLoan
    {
        _handleDodoCallback(sender, baseAmount, quoteAmount, data);
    }

    function _handleDodoCallback(address sender, uint256 baseAmount, uint256 quoteAmount, bytes calldata data) internal {
        if (_ctx.provider != LoanProvider.DODO) revert UnauthorizedProvider();
        if (msg.sender != _ctx.loanPool) revert UnauthorizedPool();
        // DODO passes the original caller (address(this)) as sender.
        if (sender != address(this)) revert UnauthorizedCallback();

        (RouteData memory route, uint256 minProfit, address loanToken, uint256 loanAmount) =
            abi.decode(data, (RouteData, uint256, address, uint256));
        if (minProfit != _ctx.minProfitWei) revert InvalidRoute();
        if (loanToken != _ctx.loanToken) revert InvalidLoanToken();
        if (loanAmount != _ctx.loanAmount) revert InvalidLoanAmount();
        if (route.pools.length != _ctx.route.pools.length) revert InvalidRoute();
        for (uint i = 0; i < route.pools.length; i++) {
            if (route.pools[i] != _ctx.route.pools[i]) revert InvalidRoute();
        }

        if ((baseAmount > 0 && quoteAmount > 0) || (baseAmount == 0 && quoteAmount == 0))
            revert InvalidBorrowedAsset();

        IDODO pool = IDODO(msg.sender);
        address base = pool._BASE_TOKEN_();
        address quote = pool._QUOTE_TOKEN_();
        address borrowedToken;
        uint256 borrowedAmount;
        if (baseAmount > 0) {
            borrowedToken = base;
            borrowedAmount = baseAmount;
        } else {
            borrowedToken = quote;
            borrowedAmount = quoteAmount;
        }

        if (borrowedToken != _ctx.loanToken) revert InvalidLoanToken();
        if (borrowedAmount != _ctx.loanAmount) revert InvalidLoanAmount();

        _ctx.state = ExecutionState.SWAP_ACTIVE;
        _executeRoute();

        // DODO flash loans have no separate fee; repay the borrowed amount directly.
        _safeTransfer(IERC20(_ctx.loanToken), msg.sender, borrowedAmount);

        _ctx.state = ExecutionState.COMPLETE;
        _finalize();
    }

    // ---- V3 swap callbacks ----
    function uniswapV3SwapCallback(int256 amount0Delta, int256 amount1Delta, bytes calldata)
        external override onlyActiveSwap
    {
        uint hop = _ctx.hop;
        DexType expectedDex = _ctx.route.dexTypes[hop];
        if (expectedDex != DexType.UNISWAP_V3) revert InvalidDexTypeForCallback();

        // Guard against multiple callbacks for the same hop.
        if (_ctx.hopPaid) revert SwapExecutionFailed();
        _ctx.hopPaid = true;

        address pool = msg.sender;
        address expectedPool = _ctx.route.pools[hop];
        if (pool != expectedPool) revert UnauthorizedPool();

        (address token0, address token1, ) = _getPoolInfoV3(pool);
        bool zeroForOne = _ctx.route.zeroForOnes[hop];

        uint256 amountToPay;
        address tokenToPay;
        if (amount0Delta > 0) {
            tokenToPay = token0;
            amountToPay = uint256(amount0Delta);
        } else if (amount1Delta > 0) {
            tokenToPay = token1;
            amountToPay = uint256(amount1Delta);
        } else {
            revert SwapExecutionFailed();
        }

        address expectedInputToken = zeroForOne ? token0 : token1;
        if (tokenToPay != expectedInputToken) revert SwapExecutionFailed();

        _safeTransfer(IERC20(tokenToPay), pool, amountToPay);
    }

    function pancakeV3SwapCallback(int256 amount0Delta, int256 amount1Delta, bytes calldata)
        external override onlyActiveSwap
    {
        uint hop = _ctx.hop;
        DexType expectedDex = _ctx.route.dexTypes[hop];
        if (expectedDex != DexType.PANCAKE_V3) revert InvalidDexTypeForCallback();

        // Guard against multiple callbacks for the same hop.
        if (_ctx.hopPaid) revert SwapExecutionFailed();
        _ctx.hopPaid = true;

        address pool = msg.sender;
        address expectedPool = _ctx.route.pools[hop];
        if (pool != expectedPool) revert UnauthorizedPool();

        (address token0, address token1, ) = _getPoolInfoV3(pool);
        bool zeroForOne = _ctx.route.zeroForOnes[hop];

        uint256 amountToPay;
        address tokenToPay;
        if (amount0Delta > 0) {
            tokenToPay = token0;
            amountToPay = uint256(amount0Delta);
        } else if (amount1Delta > 0) {
            tokenToPay = token1;
            amountToPay = uint256(amount1Delta);
        } else {
            revert SwapExecutionFailed();
        }

        address expectedInputToken = zeroForOne ? token0 : token1;
        if (tokenToPay != expectedInputToken) revert SwapExecutionFailed();

        _safeTransfer(IERC20(tokenToPay), pool, amountToPay);
    }

    // ---- Internal route execution ----
    function _executeRoute() internal {
        uint numHops = _ctx.route.pools.length;

        // Validate route endpoints: first hop must consume the loan token,
        // last hop must produce the loan token.
        {
            (address t0, address t1, ) = _getPoolInfo(_ctx.route.dexTypes[0], _ctx.route.pools[0]);
            address firstIn = _ctx.route.zeroForOnes[0] ? t0 : t1;
            if (firstIn != _ctx.loanToken) revert TokenContinuityFailed();

            uint last = numHops - 1;
            (address l0, address l1, ) = _getPoolInfo(_ctx.route.dexTypes[last], _ctx.route.pools[last]);
            address lastOut = _ctx.route.zeroForOnes[last] ? l1 : l0;
            if (lastOut != _ctx.loanToken) revert TokenContinuityFailed();
        }

        uint256 amountIn = _ctx.route.amountIn;

        for (uint i = 0; i < numHops; i++) {
            // Check deadline before each hop
            if (block.timestamp > _ctx.deadline) revert StaleTransaction();

            _ctx.hop = i;
            uint256 amountOut = _swapHop(i, amountIn);

            if (amountOut < _ctx.route.minOuts[i]) revert InsufficientOutput(amountOut, _ctx.route.minOuts[i]);

            amountIn = amountOut;
        }
    }

    function _swapHop(uint hop, uint256 amountIn) internal returns (uint256 actualOut) {
        address pool = _ctx.route.pools[hop];
        DexType dexType = _ctx.route.dexTypes[hop];
        bool zeroForOne = _ctx.route.zeroForOnes[hop];

        (address token0, address token1, ) = _getPoolInfo(dexType, pool);
        (address tokenIn, address tokenOut) = zeroForOne ? (token0, token1) : (token1, token0);

        // Continuity check
        if (hop > 0) {
            address prevPool = _ctx.route.pools[hop - 1];
            bool prevZeroForOne = _ctx.route.zeroForOnes[hop - 1];
            (address prevToken0, address prevToken1,) = _getPoolInfo(_ctx.route.dexTypes[hop - 1], prevPool);
            address prevTokenOut = prevZeroForOne ? prevToken1 : prevToken0;
            if (tokenIn != prevTokenOut) revert TokenContinuityFailed();
        }

        // Stable pool check for Aerodrome V2
        if (dexType == DexType.AERODROME_V2) {
            if (_isAerodromeStable(pool)) revert UnsupportedPoolType();
        }

        // If this is a V2 pool and marked as stable via admin, revert.
        if (dexType == DexType.AERODROME_V2 || dexType == DexType.ALIENBASE_V2) {
            if (stablePools[pool]) revert UnsupportedPoolType();
        }

        uint256 balanceBefore = IERC20(tokenOut).balanceOf(address(this));

        if (dexType == DexType.UNISWAP_V3 || dexType == DexType.PANCAKE_V3) {
            if (amountIn > uint256(type(int256).max)) revert SwapExecutionFailed();
            int256 amountSpecified = int256(amountIn);
            uint160 sqrtPriceLimitX96 = zeroForOne ? 4295128740 : 1461446703485210103287273052203988822378723970341;

            // Reset per-hop callback guard before the swap.
            _ctx.hopPaid = false;

            if (dexType == DexType.UNISWAP_V3) {
                IUniswapV3Pool(pool).swap(
                    address(this),
                    zeroForOne,
                    amountSpecified,
                    sqrtPriceLimitX96,
                    bytes("")
                );
            } else {
                IPancakeV3Pool(pool).swap(
                    address(this),
                    zeroForOne,
                    amountSpecified,
                    sqrtPriceLimitX96,
                    bytes("")
                );
            }
        } else {
            // V2 style: constant‑product (Aerodrome volatile & AlienBase).
            // Use per‑pool fee if set, otherwise default 30 bps.
            (uint112 reserve0, uint112 reserve1, ) = IUniswapV2Pair(pool).getReserves();
            (uint256 reserveIn, uint256 reserveOut) = zeroForOne ? (reserve0, reserve1) : (reserve1, reserve0);

            uint256 feeBps = poolFeeBps[pool];
            if (feeBps == 0) feeBps = 30; // default 0.3%

            // Use safe multiplication to avoid overflow.
            uint256 numerator = mulDiv(reserveOut, amountIn, 1);
            numerator = mulDiv(numerator, (10000 - feeBps), 1);
            uint256 denominator = reserveIn * 10000 + amountIn * (10000 - feeBps);
            uint256 amountOut = numerator / denominator;

            // Transfer input tokens to the pool
            _safeTransfer(IERC20(tokenIn), pool, amountIn);

            // Execute swap
            if (zeroForOne) {
                IUniswapV2Pair(pool).swap(0, amountOut, address(this), bytes(""));
            } else {
                IUniswapV2Pair(pool).swap(amountOut, 0, address(this), bytes(""));
            }
        }

        uint256 balanceAfter = IERC20(tokenOut).balanceOf(address(this));
        actualOut = balanceAfter - balanceBefore;

        if (actualOut < _ctx.route.minOuts[hop]) revert InsufficientOutput(actualOut, _ctx.route.minOuts[hop]);

        return actualOut;
    }

    // ---- Safe ERC20 transfer ----
    function _safeTransfer(IERC20 token, address to, uint256 amount) internal {
        (bool success, bytes memory data) = address(token).call(
            abi.encodeWithSelector(token.transfer.selector, to, amount)
        );
        require(success && (data.length == 0 || abi.decode(data, (bool))), "SafeERC20: transfer failed");
    }

    // ---- Safe multiplication/division ----
    function mulDiv(uint256 a, uint256 b, uint256 denominator) internal pure returns (uint256 result) {
        // 512-bit multiply [prod1 prod0] = a * b
        uint256 prod0;
        uint256 prod1;
        assembly {
            let mm := mulmod(a, b, not(0))
            prod0 := mul(a, b)
            prod1 := sub(sub(mm, prod0), lt(mm, prod0))
        }
        if (prod1 == 0) {
            require(denominator > 0);
            assembly {
                result := div(prod0, denominator)
            }
            return result;
        }
        require(denominator > prod1);
        assembly {
            result := div(prod0, denominator)
        }
        return result;
    }

    // ---- Aerodrome stable pool detection ----
    function _isAerodromeStable(address pool) internal view returns (bool) {
        (bool success, bytes memory data) = pool.staticcall(abi.encodeWithSignature("stable()"));
        if (!success) return false;
        if (data.length < 32) return false;
        return abi.decode(data, (bool));
    }

    // ---- Helpers ----
    function _getPoolInfo(DexType dexType, address pool) internal view returns (address token0, address token1, uint24 fee) {
        if (dexType == DexType.UNISWAP_V3 || dexType == DexType.PANCAKE_V3) {
            (token0, token1, fee) = _getPoolInfoV3(pool);
        } else {
            token0 = IUniswapV2Pair(pool).token0();
            token1 = IUniswapV2Pair(pool).token1();
            fee = 3000; // not used for V2
        }
    }

    function _getPoolInfoV3(address pool) internal view returns (address token0, address token1, uint24 fee) {
        token0 = IUniswapV3Pool(pool).token0();
        token1 = IUniswapV3Pool(pool).token1();
        fee = IUniswapV3Pool(pool).fee();
    }

    function _checkProfit() internal view returns (uint256 net) {
        uint256 finalBalance = IERC20(_ctx.loanToken).balanceOf(address(this));
        if (finalBalance < _ctx.initialBalance) revert InsufficientProfit(0, _ctx.minProfitWei);
        net = finalBalance - _ctx.initialBalance;
        if (net < _ctx.minProfitWei) revert InsufficientProfit(net, _ctx.minProfitWei);
    }

    function _finalize() internal {
        uint256 profit = _checkProfit();
        emit ArbitrageExecuted(_ctx.loanToken, _ctx.loanAmount, profit, _ctx.minProfitWei, true);
        delete _ctx;
        _ctx.state = ExecutionState.IDLE;
        _ctx.hopPaid = false;
    }

    receive() external payable {}
}