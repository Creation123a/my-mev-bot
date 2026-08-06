// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "forge-std/Script.sol";
import "forge-std/console.sol";
import "./FlashArbExecutor.sol";

contract DeployFlashArbExecutor is Script {
    address constant BALANCER_VAULT = 0xBA12222222228d8Ba445958a75a0704d566BF2C8;

    function run() external {
        require(block.chainid == 8453, "wrong chain");

        // Read private key as a string and normalize to a hex string with 0x prefix.
        string memory pkStr = vm.envString("PRIVATE_KEY");
        bytes memory pkBytes = bytes(pkStr);
        if (pkBytes.length >= 2 && pkBytes[0] == '0' && (pkBytes[1] == 'x' || pkBytes[1] == 'X')) {
            // Already has 0x prefix; keep as is.
        } else {
            // Prepend 0x for hex parsing.
            pkStr = string(abi.encodePacked("0x", pkStr));
        }
        uint256 pk = vm.parseUint(pkStr);

        address deployer = vm.addr(pk);
        require(deployer != address(0), "zero deployer");
        require(BALANCER_VAULT.code.length > 0, "Balancer Vault missing");

        console.log("Deployer:", deployer);
        console.log("Chain:", block.chainid);
        console.log("Balancer Vault:", BALANCER_VAULT);

        vm.startBroadcast(pk);
        FlashArbExecutor executor = new FlashArbExecutor(BALANCER_VAULT);
        vm.stopBroadcast();

        console.log("Executor:", address(executor));
        console.log("Owner:", deployer);
        console.log("DODO disabled. Call whitelistDodoPool() to enable.");
    }
}
