// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "./Interfaces.sol";

// =============================================================================
// Top-Level Structural Specifications (Must reside outside contract body)
// =============================================================================

struct PoolKey {
    address currency0;
    address currency1;
    uint24 fee;
    int24 tickSpacing;
    address hooks;
}

struct SwapParams {
    bool zeroForOne;
    int256 amountSpecified;
    uint160 sqrtPriceLimitX96;
}

interface IPoolManager {
    function unlock(bytes calldata data) external returns (bytes memory);
    function swap(PoolKey calldata key, SwapParams calldata params, bytes calldata hookData) external returns (BalanceDelta delta);
    function settle(address currency) external returns (uint256);
    function take(address currency, address to, uint256 amount) external;
}

// =============================================================================
// Uniswap V4 BalanceDelta Packing & Extraction Library
// =============================================================================

type BalanceDelta is int256;

library BalanceDeltaReader {
    function amount0(BalanceDelta delta) internal pure returns (int128) {
        int256 raw = BalanceDelta.unwrap(delta);
        int128 _amount0;
        assembly {
            _amount0 := sar(128, raw)
        }
        return _amount0;
    }
    function amount1(BalanceDelta delta) internal pure returns (int128) {
        int256 raw = BalanceDelta.unwrap(delta);
        int128 _amount1;
        assembly {
            _amount1 := and(raw, 0xffffffffffffffffffffffffffffffff)
        }
        return _amount1;
    }
}

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
    error V4CallbackInvalid();

    // ================================================================
    // Immutable addresses
    // ================================================================
    address public immutable owner;
    address public immutable balancerVault;
    address public immutable dodoPool;
    address public constant WETH = 0x4200000000000000000000000000000000000006;
    address public constant UNISWAP_V3_FACTORY = 0x33128a8fC17869897dcE68Ed026d694621f6FDfD;
    
    // Core Platform Targets (2026 Production Meta)
    address public constant VIRTUALS_FACTORY = 0x1A540088125d00dD3990f9dA45CA0859af4d3B01;
    address public constant MOLTMOON_FACTORY = 0xC68007C16088d228EF0DF92dB6A9FA19F57b9A23;
    address public constant BASEMEME_FACTORY = 0x7706d3389A197D667793Fe4991A5406085FFdfD6;
    address public constant CLAWLAUNCH_FACTORY = 0x5C0Ce7E1df7bE75E4De827E6A94EFE6F0764D00b;
    address public constant PUMPFUN_FACTORY = 0x3c267B8053683A3FeE9dbDEAA65e06a3e6A6133B;
    address public constant THRYX_FACTORY = 0x8FA4b802779BBe63ffE72b947f9FBE676A3D801a;

    // Execution Helpers & Pair Gateways
    uint160 constant MIN_SQRT_PRICE = 4295128739;
    uint160 constant MAX_SQRT_PRICE = 1461446703485210103287273052203988822378723970341;
    address public constant VIRTUAL_TOKEN = 0x0b3e328455c4059eeb9e3f84b5543f74e24e7e1b;
    address public constant VIRTUALS_WETH_POOL = 0x05b293B3306B626C1F5781dB12E8302195dfbDb1;
    
    // Uniswap V4 Canonical Singleton Address
    address public constant UNISWAP_V4_POOL_MANAGER = 0x360E68fa3b25f54313B445778aF66db9FB323A4c;

    // ================================================================
    // Context State & Operational Variables
    // ================================================================
    bytes private callbackContext;
    address private _expectedPool; 
    bool private _inFlashLoan;
    bool private _v4CallbackUsed;
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
    // Execution Routing
    // ================================================================
    function executeBondingArbitrage(
        address factory,
        address token,
        address baseToken,
        uint256 amount,
        uint256 deadline,
        uint24 fee,
        uint256 minAmountOut
    ) external onlyOwner noReentrancy {
        if (block.timestamp > deadline) revert StaleTransaction();
        if (factory != VIRTUALS_FACTORY &&
            factory != MOLTMOON_FACTORY && factory != CLAWLAUNCH_FACTORY &&
            factory != BASEMEME_FACTORY && factory != PUMPFUN_FACTORY &&
            factory != THRYX_FACTORY) revert InvalidFactory();
        if (token == address(0) || baseToken != WETH) revert InvalidToken();
        if (amount == 0) revert InvalidAmount();

        address[] memory tokens = new address[](1);
        tokens[0] = baseToken;
        uint256[] memory amounts = new uint256[](1);
        amounts[0] = amount;
        bytes memory userData = abi.encode(factory, token, baseToken, amount, fee, minAmountOut, deadline);
        IBalancerVault(balancerVault).flashLoan(address(this), tokens, amounts, userData);
    }

    function receiveFlashLoan(
        address[] memory tokens,
        uint256[] memory amounts,
        uint256[] memory feeAmounts,
        bytes memory userData
    ) external override onlyBalancer noReentrancy {
        (address factory, address token, address baseToken, uint256 loanAmount, uint24 fee, uint256 minAmountOut, uint256 deadline) =
            abi.decode(userData, (address, address, address, uint256, uint24, uint256, uint256));

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
    // DODO Interface Bridges
    // ================================================================
    function DVMFlashLoanCall(address sender, uint256 baseAmount, uint256 quoteAmount, bytes calldata data) external override onlyDodo noReentrancy {
        _handleDodoFlashLoan(sender, baseAmount, quoteAmount, data);
    }
    function DPPFlashLoanCall(address sender, uint256 baseAmount, uint256 quoteAmount, bytes calldata data) external override onlyDodo noReentrancy {
        _handleDodoFlashLoan(sender, baseAmount, quoteAmount, data);
    }
    function DSPFlashLoanCall(address sender, uint256 baseAmount, uint256 quoteAmount, bytes calldata data) external override onlyDodo noReentrancy {
        _handleDodoFlashLoan(sender, baseAmount, quoteAmount, data);
    }

    function _handleDodoFlashLoan(address, uint256, uint256, bytes calldata data) internal {
        (address factory, address token, address baseToken, uint256 loanAmount, uint24 fee, uint256 minAmountOut, uint256 deadline) =
            abi.decode(data, (address, address, address, uint256, uint24, uint256, uint256));

        if (block.timestamp > deadline) revert StaleTransaction();

        uint256 profit = _executeArbitrage(factory, token, baseToken, loanAmount, fee, minAmountOut);
        _safeTransfer(IERC20(baseToken), dodoPool, loanAmount);

        if (profit > 0) {
            _safeTransfer(IERC20(baseToken), owner, profit);
            emit BondingArbitrageExecuted(token, profit, loanAmount);
        }
    }

    // ================================================================
    // Core Engine Logic
    // ================================================================
    function _executeArbitrage(
        address factory,
        address token,
        address baseToken,
        uint256 loanAmount,
        uint24 fee,
        uint256 minAmountOut
    ) internal returns (uint256 profit) {
        uint256 tokensBought = _buyOnBondingCurve(factory, token, loanAmount, minAmountOut);
        if (tokensBought == 0) revert InsufficientBuyAmount();
        if (tokensBought < minAmountOut) revert SlippageTooHigh();

        uint256 baseReceived = _swapTokensForBase(factory, token, tokensBought, baseToken, fee);

        if (baseReceived <= loanAmount) revert InsufficientProfit(baseReceived - loanAmount, 0);
        profit = baseReceived - loanAmount;
    }

    // ================================================================
    // Inbound Platform Integrations
    // ================================================================
    function _buyOnBondingCurve(address factory, address token, uint256 amount, uint256 minAmountOut) internal returns (uint256) {
        if (factory == VIRTUALS_FACTORY) {
            uint256 balanceBefore = IERC20(token).balanceOf(address(this));
            uint256 virtualsAcquired = _swapWethForVirtuals(amount);
            _safeApprove(IERC20(VIRTUAL_TOKEN), factory, virtualsAcquired);
            (bool success, bytes memory data) = factory.call(
                abi.encodeWithSignature("buy(address,uint256)", token, virtualsAcquired)
            );
            if (!success) revert MigrationFailed();
            uint256 tokensBought;
            if (data.length >= 32) {
                tokensBought = abi.decode(data, (uint256));
            } else {
                tokensBought = IERC20(token).balanceOf(address(this)) - balanceBefore;
            }
            if (tokensBought == 0) revert InsufficientBuyAmount();
            return tokensBought;
        }
        if (factory == MOLTMOON_FACTORY || factory == PUMPFUN_FACTORY || factory == THRYX_FACTORY) {
            uint256 balanceBefore = IERC20(token).balanceOf(address(this));
            _safeApprove(IERC20(WETH), factory, 0);
            _safeApprove(IERC20(WETH), factory, amount);
            (bool success, bytes memory data) = factory.call(
                abi.encodeWithSignature("buy(address,uint256)", token, amount)
            );
            if (!success) revert MigrationFailed();
            uint256 tokensBought;
            if (data.length >= 32) {
                tokensBought = abi.decode(data, (uint256));
            } else {
                tokensBought = IERC20(token).balanceOf(address(this)) - balanceBefore;
            }
            if (tokensBought == 0) revert InsufficientBuyAmount();
            return tokensBought;
        }
        if (factory == CLAWLAUNCH_FACTORY || factory == BASEMEME_FACTORY) {
            uint256 balanceBefore = IERC20(token).balanceOf(address(this));
            _safeApprove(IERC20(WETH), factory, 0);
            _safeApprove(IERC20(WETH), factory, amount);
            (bool success, bytes memory data) = factory.call(
                abi.encodeWithSignature("buy(address,uint256,uint256)", token, amount, minAmountOut)
            );
            if (!success) revert MigrationFailed();
            uint256 tokensBought;
            if (data.length >= 32) {
                tokensBought = abi.decode(data, (uint256));
            } else {
                tokensBought = IERC20(token).balanceOf(address(this)) - balanceBefore;
            }
            if (tokensBought == 0) revert InsufficientBuyAmount();
            return tokensBought;
        }
        revert InvalidFactory();
    }

    function _swapWethForVirtuals(uint256 amountIn) internal returns (uint256 virtualsOut) {
        uint256 balanceBefore = IERC20(VIRTUAL_TOKEN).balanceOf(address(this));
        _safeApprove(IERC20(WETH), VIRTUALS_WETH_POOL, amountIn);
        _expectedPool = VIRTUALS_WETH_POOL;
        IUniswapV3Pool(VIRTUALS_WETH_POOL).swap(address(this), true, int256(amountIn), MIN_SQRT_PRICE + 1, "");
        _expectedPool = address(0);
        uint256 balanceAfter = IERC20(VIRTUAL_TOKEN).balanceOf(address(this));
        virtualsOut = balanceAfter - balanceBefore;
        if (virtualsOut == 0) revert SwapFailed();
        return virtualsOut;
    }

    function _computeUniswapV3PoolAddress(address tokenA, address tokenB, uint24 fee) internal pure returns (address pool) {
        bytes32 initCodeHash = 0xe34f199b19b2b4f47f68442619d555527d244f78a3297ea89325f843f87b8b54;
        (address token0, address token1) = tokenA < tokenB ? (tokenA, tokenB) : (tokenB, tokenA);
        bytes32 salt = keccak256(abi.encode(token0, token1, fee));
        pool = address(uint160(uint256(keccak256(abi.encodePacked(hex"ff", UNISWAP_V3_FACTORY, salt, initCodeHash)))));
    }

    // =============================================================================
    // Liquidation Routing Matrix (V3 / V4 Execution Engines)
    // =============================================================================
    function _swapTokensForBase(address factory, address tokenIn, uint256 amountIn, address baseToken, uint24 fee) internal returns (uint256 baseOut) {
        if (factory == BASEMEME_FACTORY || factory == CLAWLAUNCH_FACTORY) {
            return _swapV4TokensForBase(factory, tokenIn, amountIn, fee);
        }
        address pool = _computeUniswapV3PoolAddress(tokenIn, baseToken, fee);
        uint256 liquidity = IUniswapV3Pool(pool).liquidity();
        if (liquidity == 0) revert ZeroLiquidity();
        address token0 = IUniswapV3Pool(pool).token0();
        bool zeroForOne = tokenIn == token0;
        _expectedPool = pool;
        _safeApprove(IERC20(tokenIn), pool, amountIn);
        uint256 baseBefore = IERC20(baseToken).balanceOf(address(this));
        IUniswapV3Pool(pool).swap(address(this), zeroForOne, int256(amountIn), zeroForOne ? MIN_SQRT_PRICE + 1 : MAX_SQRT_PRICE - 1, "");
        _expectedPool = address(0);
        uint256 baseAfter = IERC20(baseToken).balanceOf(address(this));
        baseOut = baseAfter - baseBefore;
        if (baseOut == 0) revert SwapFailed();
    }

    function _swapV4TokensForBase(address factory, address token, uint256 tokensBought, uint24 fee) internal returns (uint256 baseReceived) {
        bool isToken0 = token < WETH;
        address currency0 = isToken0 ? token : WETH;
        address currency1 = isToken0 ? WETH : token;
        PoolKey memory key = PoolKey({
            currency0: currency0,
            currency1: currency1,
            fee: fee,
            tickSpacing: 60,
            hooks: factory
        });
        SwapParams memory params = SwapParams({
            zeroForOne: isToken0,
            amountSpecified: -int256(tokensBought),
            sqrtPriceLimitX96: isToken0 ? MIN_SQRT_PRICE + 1 : MAX_SQRT_PRICE - 1
        });
        callbackContext = abi.encode(key, params);
        address inputToken = isToken0 ? currency0 : currency1;
        // Approve the PoolManager to pull the input token
        _safeApprove(IERC20(inputToken), UNISWAP_V4_POOL_MANAGER, tokensBought);
        uint256 wethBalanceBefore = IERC20(WETH).balanceOf(address(this));
        IPoolManager(UNISWAP_V4_POOL_MANAGER).unlock("");
        delete callbackContext;
        uint256 wethBalanceAfter = IERC20(WETH).balanceOf(address(this));
        return wethBalanceAfter - wethBalanceBefore;
    }

    function unlockCallback(bytes calldata) external returns (bytes memory) {
if (msg.sender != UNISWAP_V4_POOL_MANAGER) revert V4CallbackInvalid();
    if (callbackContext.length == 0) revert V4CallbackInvalid();
    if (_v4CallbackUsed) revert V4CallbackInvalid();
    _v4CallbackUsed = true;

        using BalanceDeltaReader for BalanceDelta;

        (PoolKey memory key, SwapParams memory params) = abi.decode(callbackContext, (PoolKey, SwapParams));

        address inputToken = params.zeroForOne ? key.currency0 : key.currency1;
        address outputToken = params.zeroForOne ? key.currency1 : key.currency0;
        uint256 inputAmount = uint256(-params.amountSpecified);

        // 1. Execute swap FIRST to establish balance obligations on the ledger
        BalanceDelta delta = IPoolManager(UNISWAP_V4_POOL_MANAGER).swap(key, params, "");

        // 2. Settle the input token debt (manager pulls tokens via the approval)
        IPoolManager(UNISWAP_V4_POOL_MANAGER).settle(inputToken);

        // 3. Extract the precise profit delta from the packed 256-bit register
        int128 outputDelta = params.zeroForOne ? delta.amount1() : delta.amount0();

        // 4. Withdraw WETH profits (negative = owed to us)
        if (outputDelta < 0) {
            IPoolManager(UNISWAP_V4_POOL_MANAGER).take(outputToken, address(this), uint256(-int256(outputDelta)));
        } else {
            revert SwapFailed(); // Should never happen if swap succeeded
        }
_v4CallbackUsed = false;
        return "";
    }

    function uniswapV3SwapCallback(int256 amount0Delta, int256 amount1Delta, bytes calldata) external override {
        if (msg.sender != _expectedPool) revert UnauthorizedPool();
        if (amount0Delta > 0) {
            _safeTransfer(IERC20(IUniswapV3Pool(msg.sender).token0()), msg.sender, uint256(amount0Delta));
        } else if (amount1Delta > 0) {
            _safeTransfer(IERC20(IUniswapV3Pool(msg.sender).token1()), msg.sender, uint256(amount1Delta));
        }
    }

    // ================================================================
    // Internal ERC20 Utilities
    // ================================================================
    function _safeTransfer(IERC20 token, address to, uint256 amount) internal {
        (bool success, bytes memory data) = address(token).call(abi.encodeWithSelector(token.transfer.selector, to, amount));
        require(success && (data.length == 0 || abi.decode(data, (bool))), "SafeERC20: transfer failed");
    }

    function _safeApprove(IERC20 token, address spender, uint256 amount) internal {
        (bool success, bytes memory data) = address(token).call(abi.encodeWithSelector(token.approve.selector, spender, amount));
        require(success && (data.length == 0 || abi.decode(data, (bool))), "SafeERC20: approve failed");
    }

    function withdrawToken(address token) external onlyOwner {
        uint256 balance = IERC20(token).balanceOf(address(this));
        if (balance > 0) _safeTransfer(IERC20(token), owner, balance);
    }

    receive() external payable {}
}
