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

    // ================================================================
    // UPDATED: RouteData now includes full token path and per‑hop fees
    // ================================================================
    struct RouteData {
        address[] pools;        // pool address for each hop
        address[] tokens;       // full token path (length = pools.length + 1)
        DexType[] dexTypes;     // DEX type for each hop
        bool[] zeroForOnes;     // swap direction for each hop
        uint256[] minOuts;      // minimum output for each hop
        uint256[] feeBps;       // fee in basis points for each hop (V2 only; V3 ignores)
        uint256 amountIn;       // amount to borrow / input for first hop
    }

    address public immutable owner;
    IBalancerVault public immutable balancerVault;
    mapping(address => bool) public whitelistedDodoPools;

    // (Removed poolFeeBps – now passed in RouteData)

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
        bool hopPaid;
    }
    ExecutionContext private _ctx;

    event ArbitrageExecuted(
        address indexed loanToken,
        uint256 loanAmount,
        uint256 profit,
        uint256 minProfit,
        bool success
    );
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

    // ---- Owner admin ----
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
        if (balance > 0) _safeTransfer(IERC20(token), owner, balance);
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
        if (route.tokens.length != numHops + 1) revert InvalidRoute();
        if (route.dexTypes.length != numHops ||
            route.zeroForOnes.length != numHops ||
            route.minOuts.length != numHops ||
            route.feeBps.length != numHops) revert InvalidRoute();

        if (loanAmount != route.amountIn) revert InvalidLoanAmount();
        if (route.tokens[0] != loanToken || route.tokens[numHops] != loanToken) revert InvalidRoute();

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
        address borrowedToken = (baseAmount > 0) ? base : quote;
        uint256 borrowedAmount = (baseAmount > 0) ? baseAmount : quoteAmount;

        if (borrowedToken != _ctx.loanToken) revert InvalidLoanToken();
        if (borrowedAmount != _ctx.loanAmount) revert InvalidLoanAmount();

        _ctx.state = ExecutionState.SWAP_ACTIVE;
        _executeRoute();

        _safeTransfer(IERC20(_ctx.loanToken), msg.sender, borrowedAmount);

        _ctx.state = ExecutionState.COMPLETE;
        _finalize();
    }

    // ---- V3 callbacks (updated to use token path, no staticcalls) ----
    function uniswapV3SwapCallback(int256 amount0Delta, int256 amount1Delta, bytes calldata)
        external override onlyActiveSwap
    {
        uint hop = _ctx.hop;
        DexType expectedDex = _ctx.route.dexTypes[hop];
        if (expectedDex != DexType.UNISWAP_V3) revert InvalidDexTypeForCallback();

        if (_ctx.hopPaid) revert SwapExecutionFailed();
        _ctx.hopPaid = true;

        address pool = msg.sender;
        if (pool != _ctx.route.pools[hop]) revert UnauthorizedPool();

        // Determine token0 and token1 from the token path and zeroForOne flag.
        address tokenIn = _ctx.route.tokens[hop];
        address tokenOut = _ctx.route.tokens[hop + 1];
        bool zeroForOne = _ctx.route.zeroForOnes[hop];
        // If zeroForOne is true, tokenIn is token0, tokenOut is token1; else reversed.
        (address token0, address token1) = zeroForOne ? (tokenIn, tokenOut) : (tokenOut, tokenIn);

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

        if (_ctx.hopPaid) revert SwapExecutionFailed();
        _ctx.hopPaid = true;

        address pool = msg.sender;
        if (pool != _ctx.route.pools[hop]) revert UnauthorizedPool();

        address tokenIn = _ctx.route.tokens[hop];
        address tokenOut = _ctx.route.tokens[hop + 1];
        bool zeroForOne = _ctx.route.zeroForOnes[hop];
        (address token0, address token1) = zeroForOne ? (tokenIn, tokenOut) : (tokenOut, tokenIn);

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

    // ---- Internal route execution (no _getPoolInfo calls) ----
    function _executeRoute() internal {
        uint numHops = _ctx.route.pools.length;
        // Validate endpoints using token path.
        if (_ctx.route.tokens[0] != _ctx.loanToken) revert TokenContinuityFailed();
        if (_ctx.route.tokens[numHops] != _ctx.loanToken) revert TokenContinuityFailed();

        uint256 amountIn = _ctx.route.amountIn;

        for (uint i = 0; i < numHops; i++) {
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
        address tokenIn = _ctx.route.tokens[hop];
        address tokenOut = _ctx.route.tokens[hop + 1];

        // Continuity check: ensure tokenIn matches previous hop's tokenOut.
        if (hop > 0) {
            if (tokenIn != _ctx.route.tokens[hop]) revert TokenContinuityFailed();
        }

        // Stable pool check for Aerodrome V2 – we still need to know if stable.
        // We can keep the staticcall because it's only for Aerodrome stable detection.
        // Alternatively, the solver could pass a flag, but we keep this minimal.
        if (dexType == DexType.AERODROME_V2) {
            if (_isAerodromeStable(pool)) revert UnsupportedPoolType();
        }

        uint256 balanceBefore = IERC20(tokenOut).balanceOf(address(this));

        if (dexType == DexType.UNISWAP_V3 || dexType == DexType.PANCAKE_V3) {
            if (amountIn > uint256(type(int256).max)) revert SwapExecutionFailed();
            int256 amountSpecified = int256(amountIn);
            uint160 sqrtPriceLimitX96 = zeroForOne ? 4295128740 : 1461446703485210103287273052203988822378723970341;

            _ctx.hopPaid = false;

            if (dexType == DexType.UNISWAP_V3) {
                IUniswapV3Pool(pool).swap(address(this), zeroForOne, amountSpecified, sqrtPriceLimitX96, bytes(""));
            } else {
                IPancakeV3Pool(pool).swap(address(this), zeroForOne, amountSpecified, sqrtPriceLimitX96, bytes(""));
            }
        } else {
            // V2 style
            uint256 reserveIn;
            uint256 reserveOut;

            if (dexType == DexType.AERODROME_V2) {
                (uint256 r0, uint256 r1, ) = IAerodromeV2Pool(pool).getReserves();
                (reserveIn, reserveOut) = zeroForOne ? (r0, r1) : (r1, r0);
            } else {
                (uint112 r0, uint112 r1, ) = IUniswapV2Pair(pool).getReserves();
                (reserveIn, reserveOut) = zeroForOne ? (uint256(r0), uint256(r1)) : (uint256(r1), uint256(r0));
            }

            // Use fee passed in RouteData, with fallback to 30 if somehow zero.
            uint256 feeBps = _ctx.route.feeBps[hop];
            if (feeBps == 0) feeBps = 30;

            uint256 numerator = mulDiv(reserveOut, amountIn, 1);
            numerator = mulDiv(numerator, (10000 - feeBps), 1);
            uint256 denominator = mulDiv(reserveIn, 10000, 1) + mulDiv(amountIn, (10000 - feeBps), 1);
            uint256 amountOut = numerator / denominator;

            _safeTransfer(IERC20(tokenIn), pool, amountIn);

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

    // ---- Aerodrome stable detection (kept minimal) ----
    function _isAerodromeStable(address pool) internal view returns (bool) {
        (bool success, bytes memory data) = pool.staticcall(abi.encodeWithSignature("stable()"));
        if (!success) return false;
        if (data.length < 32) return false;
        return abi.decode(data, (bool));
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
