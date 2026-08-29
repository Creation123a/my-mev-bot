// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "./Interfaces.sol";

contract FlashLiquidationExecutor is IBalancerFlashLoanRecipient {
    // ============================================================
    // Immutable Addresses (Hypervision Core)
    // ============================================================
    address public immutable owner;
    address public immutable balancerVault;

// Protocol Addresses (Base Mainnet 2026)
address public constant AAVE_POOL = 0xA238Dd80C259a72e81d7e4664a9801593F98d1c5;
address public constant MORPHO_BLUE = 0x34CD04070dD72b14E241112F6d83812Df5Af7fCD;
address public constant COMPOUND_COMET = 0xb125E6687d4313864e53df431d5425969c15Eb2F;
address public constant EXACTLY_AUDITOR = 0x0Aeb0BCB919858C0a4dceC3EeD879985034A597c;
address public constant MOONWELL_MTOKEN = 0xEdc817A28E8B93B03976FBd4a3dDBc9f7D176c22;
address public constant IONIC_POOL = 0x05c9C6417F246600f8f5f49fcA9Ee991bfF73D13;

// DEX Router
address public constant SWAP_ROUTER = 0x2626664c2603336E57B271c5C0b26F421741e481;

    // Reentrancy guard (same as your FlashArbExecutor)
    bool private _locked;

    modifier noReentrancy() {
        require(!_locked, "Reentrancy");
        _locked = true;
        _;
        _locked = false;
    }

    modifier onlyBalancer() {
        require(msg.sender == balancerVault, "Unauthorized callback");
        _;
    }

    constructor(address _balancerVault) {
        owner = msg.sender;
        balancerVault = _balancerVault;
    }
    // ============================================================
// Modifiers (FIX: Added missing onlyOwner)
// ============================================================
modifier onlyOwner() {
    require(msg.sender == owner, "Unauthorized");
    _;
}

    // ============================================================
    // Main Entry Point (Called by your Go bot worker4)
    // ============================================================
    function executeLiquidation(
        uint8 protocol,          // 1=Aave, 2=Compound, 3=Morpho, 4=Exactly, 5=Moonwell, 6=Ionic
        address debtAsset,
        uint256 debtAmount,
        uint24 fee,     
        bytes memory liquidationData // Protocol-specific encoded params
    ) external onlyOwner noReentrancy {
        // 1. Prepare Balancer flash loan
        address[] memory tokens = new address[](1);
        tokens[0] = debtAsset;
        uint256[] memory amounts = new uint256[](1);
        amounts[0] = debtAmount;

        // 2. Encode everything into userData for the callback
        bytes memory userData = abi.encode(protocol, debtAsset, debtAmount, fee, liquidationData);

        // 3. Fire the 0% fee Balancer flash loan
        IBalancerVault(balancerVault).flashLoan(
            address(this),
            tokens,
            amounts,
            userData
        );
    }

    // ============================================================
    // Balancer V2 Callback (0% Fee - Free Money Glitch)
    // ============================================================
    function receiveFlashLoan(
    address[] memory tokens,
    uint256[] memory amounts,
    uint256[] memory feeAmounts,
    bytes memory userData
) external override onlyBalancer noReentrancy {
    // 1. Decode the userData (NOW INCLUDES FEE)
    (uint8 protocol, address debtAsset, uint256 debtAmount, uint24 fee, bytes memory liquidationData) =
        abi.decode(userData, (uint8, address, uint256, uint24, bytes));

    address collateralAsset;
    uint256 collateralReceived;

    // 2. Route to the specific protocol liquidation logic
    if (protocol == 1) { // AAVE V3
        (address collateral, address user, uint256 debtToCover, bool receiveAToken) =
            abi.decode(liquidationData, (address, address, uint256, bool));
        collateralAsset = collateral;
        IAavePool(AAVE_POOL).liquidationCall(collateral, debtAsset, user, debtToCover, receiveAToken);
    } 
    else if (protocol == 2) { // COMPOUND III
        (address user, address collateral, uint256 minAmount) =
            abi.decode(liquidationData, (address, address, uint256));
        collateralAsset = collateral;
        address[] memory accounts = new address[](1);
        accounts[0] = user;
        IComet(COMPOUND_COMET).absorb(address(this), accounts);
        IComet(COMPOUND_COMET).buyCollateral(collateral, minAmount, debtAmount, address(this));
    } 
    else if (protocol == 3) { // MORPHO BLUE
        (IMorpho.MarketParams memory params, address borrower, uint256 seizedAssets, uint256 maxRepay) =
            abi.decode(liquidationData, (IMorpho.MarketParams, address, uint256, uint256));
        collateralAsset = params.collateralToken;
        IMorpho(MORPHO_BLUE).liquidate(params, borrower, seizedAssets, maxRepay, "");
    }
    else if (protocol == 4) { // EXACTLY
        (address borrower, address seizeMarket, uint256 maxAssets) =
            abi.decode(liquidationData, (address, address, uint256));
        collateralAsset = IExactlyMarket(seizeMarket).asset();
        IExactlyMarket(seizeMarket).liquidate(borrower, maxAssets, seizeMarket);
    }
    else if (protocol == 5) { // MOONWELL
        (address borrower, address mTokenCollateral, uint256 repayAmount) =
            abi.decode(liquidationData, (address, address, uint256));
        collateralAsset = IMoonwellMToken(mTokenCollateral).underlying();
        IMoonwellMToken(mTokenCollateral).liquidateBorrow(borrower, repayAmount, mTokenCollateral);
    } 
    else if (protocol == 6) { // IONIC
        (address borrower, address collateral, uint256 repayAmount) =
            abi.decode(liquidationData, (address, address, uint256));
        collateralAsset = collateral;
        IIonicPool(IONIC_POOL).liquidate(borrower, collateral, repayAmount);
    } 
    else {
        revert("Unsupported protocol");
    }

    // 3. Get the collateral we just received
    uint256 collateralBalance = IERC20(collateralAsset).balanceOf(address(this));
    require(collateralBalance > 0, "No collateral seized");

    // 4. Calculate min amount out with 0.5% slippage
    uint256 minAmountOut = (debtAmount * 995) / 1000;
    if (minAmountOut == 0) minAmountOut = 1;

    // 5. Swap collateral -> debtAsset using DYNAMIC FEE
    uint256 debtObtained = _swapExactTokensForTokens(
        collateralAsset,
        debtAsset,
        collateralBalance,
        minAmountOut,
        fee  // <-- PASS THE DYNAMIC FEE
    );
    require(debtObtained > debtAmount, "Not profitable");

    // 6. Repay Balancer (with fee if any)
    uint256 repayAmount = amounts[0] + feeAmounts[0];
    IERC20(debtAsset).transfer(balancerVault, repayAmount);

    // 7. Keep the profit
    uint256 profit = debtObtained - debtAmount;
    require(profit > 0, "Zero profit");
    IERC20(debtAsset).transfer(owner, profit);
}

    // ============================================================
    // Internal DEX Swapper (Aerodrome/Uniswap V3)
    // ============================================================
    function _swapExactTokensForTokens(
    address tokenIn,
    address tokenOut,
    uint256 amountIn,
    uint256 amountOutMin,
    uint24 fee  // <-- DYNAMIC FEE TIER
) internal returns (uint256 amountOut) {
    // Approve the router to spend the input token
    IERC20(tokenIn).approve(SWAP_ROUTER, amountIn);

    // Build swap params with the dynamic fee
    ISwapRouter.ExactInputSingleParams memory params = ISwapRouter.ExactInputSingleParams({
        tokenIn: tokenIn,
        tokenOut: tokenOut,
        fee: fee,  // <-- USE THE PASSED FEE (not hardcoded 3000)
        recipient: address(this),
        deadline: block.timestamp + 60,
        amountIn: amountIn,
        amountOutMinimum: amountOutMin,
        sqrtPriceLimitX96: 0
    });

    // Execute the swap
    amountOut = ISwapRouter(SWAP_ROUTER).exactInputSingle(params);
    require(amountOut >= amountOutMin, "Slippage too high");
    return amountOut;
}

    // ============================================================
    // Emergency Withdrawals
    // ============================================================
    function withdrawToken(address token) external onlyOwner {
        uint256 balance = IERC20(token).balanceOf(address(this));
        if (balance > 0) {
            IERC20(token).transfer(owner, balance);
        }
    }

    receive() external payable {}
}