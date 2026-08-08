#!/bin/bash
set -e

RPC="http://localhost:8545"
PK="ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
FROM="0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
ROUTER="0x2626664c2603336E57B271c5C0b26F421741e481"
WETH="0x4200000000000000000000000000000000000006"
USDC="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"

while true; do
  AMOUNT=$(shuf -i 100000000000000000 -e 1000000000000000000 -n 1)
  cast send $ROUTER "swapExactInputSingle(address,address,uint24,address,uint256,uint256,uint160)" \
    $WETH $USDC 3000 $FROM $AMOUNT 0 0 \
    --private-key $PK --rpc-url $RPC --gas-limit 300000 \
    > /dev/null 2>&1 || true
  sleep $(shuf -i 5-15 -n 1)
done
