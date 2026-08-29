// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "./Interfaces.sol";

contract BondingArbitrageExecutor is
    IBalancerFlashLoanRecipient,
    IDODOCallee,
    IUniswapV3SwapCallback
{
    // ================================================================
    // Custom errors
    // ================================================================
    error UnauthorizedCaller();
    error UnauthorizedFlashLoanProvider();
    error InvalidFactory();
    error InvalidToken();
    error InvalidAmount();
    error InsufficientBuyAmount();
    error MigrationFailed();
    error SwapFailed();
    error InsufficientProfit(uint256 actual, uint256 required);
    error AlreadyMigrated();
    error FlashLoanRepaymentFailed();
    error ZeroLiquidity();
    error StaleTransaction();
    error UnauthorizedPool();
    error SlippageTooHigh();

    // ================================================================
    // Immutable addresses
    // ================================================================
    address public immutable owner;
    address public immutable balancerVault;
    address public immutable dodoPool;
    address public constant WETH = 0x4200000000000000000000000000000000000006;
    address public constant UNISWAP_V3_FACTORY = 0x33128a8fC17869897dcE68Ed026d694621f6FDfD;
    address public constant CLANKER_V4_FACTORY = 0xe85a59c628f7d27878aceb4bf3b35733630083a9;
    address public constant VIRTUALS_FACTORY = 0x1A540088125d00dD3990f9dA45CA0859af4d3B01;

    // ================================================================
    // State (for callback safety)
    // ================================================================
    address private _expectedPool; // B1: expected pool for V3 callback

    // ================================================================
    // Reentrancy guard
    // ================================================================
    bool private _inFlashLoan;

    // ================================================================
    // Events
    // ================================================================
    event BondingArbitrageExecuted(address indexed token, uint256 profit, uint256 loanAmount);

    // ================================================================
    // Constructor
    // ================================================================
    constructor(address _balancerVault, address _dodoPool) {
        if (_balancerVault == address(0) || _dodoPool == address(0)) revert UnauthorizedFlashLoanProvider();
        owner = msg.sender;
        balancerVault = _balancerVault;
        dodoPool = _dodoPool;
    }

    // ================================================================
    // Modifiers
    // ================================================================
    modifier onlyOwner() {
        if (msg.sender != owner) revert UnauthorizedCaller();
        _;
    }

    modifier onlyBalancer() {
        if (msg.sender != balancerVault) revert UnauthorizedFlashLoanProvider();
        _;
    }

    modifier onlyDodo() {
        if (msg.sender != dodoPool) revert UnauthorizedFlashLoanProvider();
        _;
    }

    modifier noReentrancy() {
        require(!_inFlashLoan, "Reentrancy");
        _inFlashLoan = true;
        _;
        _inFlashLoan = false;
    }

    // ================================================================
    // Main entry point (B3: added deadline)
    // ================================================================
    function executeBondingArbitrage(
        address factory,
        address token,
        address baseToken,
        uint256 amount,
        uint256 deadline,           // B3: deadline parameter
        uint24 fee,                 // B4: fee tier (e.g., 500, 3000, 10000)
        uint256 minAmountOut        // B5: minimum tokens to buy (slippage control)
    ) external onlyOwner noReentrancy {
        if (block.timestamp > deadline) revert StaleTransaction(); // B3
        if (factory != CLANKER_V4_FACTORY && factory != VIRTUALS_FACTORY) revert InvalidFactory();
        if (token == address(0) || baseToken != WETH) revert InvalidToken();
        if (amount == 0) revert InvalidAmount();

        // Store the fee for later use
        // We'll pass it via userData or via a separate variable.
        // For simplicity, we'll encode everything in userData.
        address[] memory tokens = new address[](1);
        tokens[0] = baseToken;
        uint256[] memory amounts = new uint256[](1);
        amounts[0] = amount;
        bytes memory userData = abi.encode(factory, token, baseToken, amount, fee, minAmountOut, deadline);
        IBalancerVault(balancerVault).flashLoan(address(this), tokens, amounts, userData);
    }

    // ================================================================
    // Balancer callback
    // ================================================================
    function receiveFlashLoan(
        address[] memory tokens,
        uint256[] memory amounts,
        uint256[] memory feeAmounts,
        bytes memory userData
    ) external override onlyBalancer noReentrancy {
        (address factory, address token, address baseToken, uint256 loanAmount, uint24 fee, uint256 minAmountOut, uint256 deadline) =
            abi.decode(userData, (address, address, address, uint256, uint24, uint256, uint256));

        // B3: check deadline again inside callback (optional)
        if (block.timestamp > deadline) revert StaleTransaction();

        uint256 profit = _executeArbitrage(factory, token, baseToken, loanAmount, fee, minAmountOut);

        uint256 repayAmount = amounts[0] + feeAmounts[0];
        _safeTransfer(IERC20(baseToken), balancerVault, repayAmount);

        if (profit > 0) {
            _safeTransfer(IERC20(baseToken), owner, profit);
            emit BondingArbitrageExecuted(token, profit, loanAmount);
        }
    }

    // ================================================================
    // DODO callbacks (updated to handle fee and minAmountOut)
    // ================================================================
    function DVMFlashLoanCall(address sender, uint256 baseAmount, uint256 quoteAmount, bytes calldata data)
        external override onlyDodo noReentrancy
    {
        _handleDodoFlashLoan(sender, baseAmount, quoteAmount, data);
    }
    function DPPFlashLoanCall(address sender, uint256 baseAmount, uint256 quoteAmount, bytes calldata data)
        external override onlyDodo noReentrancy
    {
        _handleDodoFlashLoan(sender, baseAmount, quoteAmount, data);
    }
    function DSPFlashLoanCall(address sender, uint256 baseAmount, uint256 quoteAmount, bytes calldata data)
        external override onlyDodo noReentrancy
    {
        _handleDodoFlashLoan(sender, baseAmount, quoteAmount, data);
    }

    function _handleDodoFlashLoan(address sender, uint256 baseAmount, uint256 quoteAmount, bytes calldata data) internal {
        (address factory, address token, address baseToken, uint256 loanAmount, uint24 fee, uint256 minAmountOut, uint256 deadline) =
            abi.decode(data, (address, address, address, uint256, uint24, uint256, uint256));

        if (block.timestamp > deadline) revert StaleTransaction();

        uint256 profit = _executeArbitrage(factory, token, baseToken, loanAmount, fee, minAmountOut);

        // Repay DODO (we assume 0% fee; if fee exists, add it)
        // We could query the fee but for simplicity, we'll trust the pool's fee is 0.
        _safeTransfer(IERC20(baseToken), dodoPool, loanAmount);

        if (profit > 0) {
            _safeTransfer(IERC20(baseToken), owner, profit);
            emit BondingArbitrageExecuted(token, profit, loanAmount);
        }
    }

    // ================================================================
    // Core execution (B7: check tokensBought)
    // ================================================================
    function _executeArbitrage(
    address factory,
    address token,
    address baseToken,
    uint256 loanAmount,
    uint24 fee,
    uint256 minAmountOut
) internal returns (uint256 profit) {
    // 1. Buy remaining tokens
    uint256 tokensBought = _buyOnBondingCurve(factory, token, loanAmount, minAmountOut);
    if (tokensBought == 0) revert InsufficientBuyAmount(); // B7

    // ---- NEW: Slippage check after purchase ----
    if (tokensBought < minAmountOut) revert SlippageTooHigh();

    // 2. Compute pool address using the provided fee
    address pool = _computeUniswapV3PoolAddress(token, baseToken, fee);

    // 3. Swap tokens for base
    uint256 baseReceived = _swapTokensForBase(token, pool, tokensBought, baseToken);

    // 4. Profit
    if (baseReceived <= loanAmount) revert InsufficientProfit(baseReceived - loanAmount, 0);
    profit = baseReceived - loanAmount;
}

    // ================================================================
    // Bonding curve buy (B5, B6, B8)
    // ================================================================
    function _buyOnBondingCurve(address factory, address token, uint256 amount, uint256 minAmountOut)
    internal
    returns (uint256)
{
    // Clanker v4: buyToken(address token, uint256 minAmountOut)
    if (factory == CLANKER_V4_FACTORY) {
        // B8: reset allowance first (for USDT-like tokens) – now uses safe approve
        _safeApprove(IERC20(WETH), factory, 0);
        _safeApprove(IERC20(WETH), factory, amount);
        (bool success, bytes memory data) = factory.call(
            abi.encodeWithSignature("buyToken(address,uint256)", token, minAmountOut)
        );
        if (!success) revert MigrationFailed();
        if (data.length < 32) revert MigrationFailed();
        return abi.decode(data, (uint256));
    }
// Virtuals: buy(address token, uint256 amount)
else if (factory == VIRTUALS_FACTORY) {
    // Snapshot balance before buying
    uint256 balanceBefore = IERC20(token).balanceOf(address(this));
    _safeApprove(IERC20(WETH), factory, 0);
    _safeApprove(IERC20(WETH), factory, amount);
    (bool success, bytes memory data) = factory.call(
        abi.encodeWithSignature("buy(address,uint256)", token, amount)
    );
    if (!success) revert MigrationFailed();
    uint256 tokensBought;
    if (data.length >= 32) {
        // If the function returns a value, use it
        tokensBought = abi.decode(data, (uint256));
    } else {
        // Fallback: compute from balance difference
        uint256 balanceAfter = IERC20(token).balanceOf(address(this));
        tokensBought = balanceAfter - balanceBefore;
    }
    if (tokensBought == 0) revert InsufficientBuyAmount();
    return tokensBought;
}
}

    // ================================================================
    // Uniswap V3 pool address computation
    // ================================================================
    function _computeUniswapV3PoolAddress(
        address tokenA,
        address tokenB,
        uint24 fee
    ) internal pure returns (address pool) {
        bytes32 initCodeHash = 0xe34f199b19b2b4f47f68442619d555527d244f78a3297ea89325f843f87b8b54; // Uniswap V3
        (address token0, address token1) = tokenA < tokenB ? (tokenA, tokenB) : (tokenB, tokenA);
        bytes32 salt = keccak256(abi.encode(token0, token1, fee));
        pool = address(uint160(uint256(keccak256(abi.encodePacked(
            hex"ff",
            UNISWAP_V3_FACTORY,
            salt,
            initCodeHash
        )))));
    }

    // ================================================================
    // Swap on Uniswap V3 pool (B1: set expected pool)
    // ================================================================
    function _swapTokensForBase(
        address tokenIn,
        address pool,
        uint256 amountIn,
        address baseToken
    ) internal returns (uint256 baseOut) {
        uint256 liquidity = IUniswapV3Pool(pool).liquidity();
        if (liquidity == 0) revert ZeroLiquidity();

        address token0 = IUniswapV3Pool(pool).token0();
        address token1 = IUniswapV3Pool(pool).token1();
        bool zeroForOne = tokenIn == token0;

        // B1: store expected pool for callback
        _expectedPool = pool;

        _safeApprove(IERC20(tokenIn), pool, amountIn);

        uint256 baseBefore = IERC20(baseToken).balanceOf(address(this));

        IUniswapV3Pool(pool).swap(
            address(this),
            zeroForOne,
            int256(amountIn),
            zeroForOne ? 4295128740 : 1461446703485210103287273052203988822378723970341,
            ""
        );

        // Clear expected pool (optional)
        _expectedPool = address(0);

        uint256 baseAfter = IERC20(baseToken).balanceOf(address(this));
        baseOut = baseAfter - baseBefore;
        if (baseOut == 0) revert SwapFailed();
    }

    // ================================================================
    // Uniswap V3 swap callback (B1: verify caller)
    // ================================================================
    function uniswapV3SwapCallback(
        int256 amount0Delta,
        int256 amount1Delta,
        bytes calldata data
    ) external override {
        // B1: only the expected pool can call this
        if (msg.sender != _expectedPool) revert UnauthorizedPool();

        if (amount0Delta > 0) {
            _safeTransfer(IERC20(IUniswapV3Pool(msg.sender).token0()), msg.sender, uint256(amount0Delta));
        } else if (amount1Delta > 0) {
            _safeTransfer(IERC20(IUniswapV3Pool(msg.sender).token1()), msg.sender, uint256(amount1Delta));
        }
    }

    // ================================================================
    // Safe ERC20 helpers
    // ================================================================
    function _safeTransfer(IERC20 token, address to, uint256 amount) internal {
        (bool success, bytes memory data) = address(token).call(
            abi.encodeWithSelector(token.transfer.selector, to, amount)
        );
        require(success && (data.length == 0 || abi.decode(data, (bool))), "SafeERC20: transfer failed");
    }

    function _safeApprove(IERC20 token, address spender, uint256 amount) internal {
        (bool success, bytes memory data) = address(token).call(
            abi.encodeWithSelector(token.approve.selector, spender, amount)
        );
        require(success && (data.length == 0 || abi.decode(data, (bool))), "SafeERC20: approve failed");
    }

    // ================================================================
    // Emergency withdrawal
    // ================================================================
    function withdrawToken(address token) external onlyOwner {
        uint256 balance = IERC20(token).balanceOf(address(this));
        if (balance > 0) _safeTransfer(IERC20(token), owner, balance);
    }

    receive() external payable {}
}