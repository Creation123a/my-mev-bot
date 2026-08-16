// Package poolregistry provides a "Fetch-Once-and-Cache-Permanently" address registry.
package poolregistry

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"://github.com"
	"://github.com/bind"
	"://github.com"
	"://github.com"
)

var (
	UniswapV3Router   = common.HexToAddress("0x2626664c2603336E57B271c5C0b26F421741e481")
	PancakeV3Router   = common.HexToAddress("0x1b81D678ffb9C0263b24A97847620C99d213eB14")
	AerodromeV2Router = common.HexToAddress("0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43")
	AlienBaseV2Router = common.HexToAddress("0x8c1A3cF8f83074169FE5D7aD50B978e1cD6b37c7")
)

type PoolKey struct {
	TokenA string
	TokenB string
	Fee    uint32 // 0 for V2 standard
	Stable bool   // True for Aerodrome stable pools, false for volatile
}

type PoolRegistry struct {
	client      *ethclient.Client
	cache       sync.Map
	factoryMu   sync.RWMutex
	uniswapV3   common.Address
	pancakeV3   common.Address
	aerodromeV2 common.Address
	alienBaseV2 common.Address
	fetched     bool
}

func New(rpcURL string) (*PoolRegistry, error) {
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to dial RPC: %w", err)
	}
	return &PoolRegistry{client: client}, nil
}

// Safely sort addresses alphabetically to prevent duplicate entries (fixes Bug #3)
func sortTokens(a, b common.Address) (string, string) {
	aStr := strings.ToLower(a.Hex())
	bStr := strings.ToLower(b.Hex())
	if aStr < bStr {
		return aStr, bStr
	}
	return bStr, aStr
}

func (pr *PoolRegistry) EnsureFactories(ctx context.Context) error {
	pr.factoryMu.Lock()
	defer pr.factoryMu.Unlock()
	if pr.fetched {
		return nil
	}

	// Dynamic standard signature
	const factoryABI = `[{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]`
	parsed, err := abi.JSON(strings.NewReader(factoryABI))
	if err != nil {
		return fmt.Errorf("parse factory ABI: %w", err)
	}

	// Fixes Bug #1: Special signature required to read the unique Aerodrome architecture layout
	const aerodromeRouterABI = `[{"inputs":[],"name":"factory0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]`
	parsedAero, err := abi.JSON(strings.NewReader(aerodromeRouterABI))
	if err != nil {
		return fmt.Errorf("parse aero factory ABI: %w", err)
	}

	fetchMethod := func(router common.Address, abiObject abi.ABI, methodName string) (common.Address, error) {
		contract := bind.NewBoundContract(router, abiObject, pr.client, pr.client, pr.client)
		var out []interface{}
		subCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		err := contract.Call(&bind.CallOpts{Context: subCtx}, &out, methodName)
		if err != nil {
			return common.Address{}, err
		}
		if len(out) == 0 {
			return common.Address{}, fmt.Errorf("empty response")
		}
		return out[0].(common.Address), nil
	}

	if addr, err := fetchMethod(UniswapV3Router, parsed, "factory"); err == nil { pr.uniswapV3 = addr } else { return fmt.Errorf("uni v3 rpc drop: %w", err) }
	if addr, err := fetchMethod(PancakeV3Router, parsed, "factory"); err == nil { pr.pancakeV3 = addr } else { return fmt.Errorf("pancake v3 rpc drop: %w", err) }
	if addr, err := fetchMethod(AlienBaseV2Router, parsed, "factory"); err == nil { pr.alienBaseV2 = addr } else { return fmt.Errorf("alienbase v2 rpc drop: %w", err) }
	
	// Aerodrome calls 'factory0' instead of standard 'factory'
	if addr, err := fetchMethod(AerodromeV2Router, parsedAero, "factory0"); err == nil { pr.aerodromeV2 = addr } else { return fmt.Errorf("aerodrome v2 rpc drop: %w", err) }

	pr.fetched = true
	return nil
}

func (pr *PoolRegistry) UniswapV3Factory() common.Address   { pr.factoryMu.RLock(); defer pr.factoryMu.RUnlock(); return pr.uniswapV3 }
func (pr *PoolRegistry) PancakeV3Factory() common.Address   { pr.factoryMu.RLock(); defer pr.factoryMu.RUnlock(); return pr.pancakeV3 }
func (pr *PoolRegistry) AerodromeV2Factory() common.Address { pr.factoryMu.RLock(); defer pr.factoryMu.RUnlock(); return pr.aerodromeV2 }
func (pr *PoolRegistry) AlienBaseV2Factory() common.Address { pr.factoryMu.RLock(); defer pr.factoryMu.RUnlock(); return pr.alienBaseV2 }

func (pr *PoolRegistry) FetchAndCacheV3(factory, tokenA, tokenB common.Address, fee uint32) (common.Address, error) {
	t0, t1 := sortTokens(tokenA, tokenB)
	key := PoolKey{TokenA: t0, TokenB: t1, Fee: fee, Stable: false}
	if val, ok := pr.cache.Load(key); ok {
		return val.(common.Address), nil
	}

	const getPoolABI = `[{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"}],"name":"getPool","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]`
	parsedABI, err := abi.JSON(strings.NewReader(getPoolABI))
	if err != nil {
		return common.Address{}, err
	}
	contract := bind.NewBoundContract(factory, parsedABI, pr.client, pr.client, pr.client)
	var out []interface{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = contract.Call(&bind.CallOpts{Context: ctx}, &out, "getPool", common.HexToAddress(t0), common.HexToAddress(t1), fee)
	if err != nil {
		return common.Address{}, err
	}
	poolAddr := out[0].(common.Address)
	if poolAddr == (common.Address{}) {
		return common.Address{}, fmt.Errorf("v3 pool missing")
	}
	pr.cache.Store(key, poolAddr)
	return poolAddr, nil
}

// FetchAndCacheV2Standard tracks standard forks (AlienBase V2)
func (pr *PoolRegistry) FetchAndCacheV2Standard(factory, tokenA, tokenB common.Address) (common.Address, error) {
	t0, t1 := sortTokens(tokenA, tokenB)
	key := PoolKey{TokenA: t0, TokenB: t1, Fee: 0, Stable: false}
	if val, ok := pr.cache.Load(key); ok {
		return val.(common.Address), nil
	}

	const getPairABI = `[{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"getPair","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]`
	parsedABI, err := abi.JSON(strings.NewReader(getPairABI))
	if err != nil {
		return common.Address{}, err
	}
	contract := bind.NewBoundContract(factory, parsedABI, pr.client, pr.client, pr.client)
	var out []interface{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = contract.Call(&bind.CallOpts{Context: ctx}, &out, "getPair", common.HexToAddress(t0), common.HexToAddress(t1))
	if err != nil {
		return common.Address{}, err
	}
	poolAddr := out[0].(common.Address)
	if poolAddr == (common.Address{}) {
		return common.Address{}, fmt.Errorf("v2 pair missing")
	}
	pr.cache.Store(key, poolAddr)
	return poolAddr, nil
}

// FetchAndCacheAerodromeV2 resolves custom Aerodrome pool properties (Fixes Bug #2)
func (pr *PoolRegistry) FetchAndCacheAerodromeV2(factory, tokenA, tokenB common.Address, stable bool) (common.Address, error) {
	t0, t1 := sortTokens(tokenA, tokenB)
	key := PoolKey{TokenA: t0, TokenB: t1, Fee: 0, Stable: stable}
	if val, ok := pr.cache.Load(key); ok {
		return val.(common.Address), nil
	}

	const aeroABI = `[{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"bool","name":"stable","type":"bool"}],"name":"getPool","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]`
	parsedABI, err := abi.JSON(strings.NewReader(aeroABI))
	if err != nil {
		return common.Address{}, err
	}
	contract := bind.NewBoundContract(factory, parsedABI, pr.client, pr.client, pr.client)
	var out []interface{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = contract.Call(&bind.CallOpts{Context: ctx}, &out, "getPool", common.HexToAddress(t0), common.HexToAddress(t1), stable)
	if err != nil {
		return common.Address{}, err
	}
	poolAddr := out[0].(common.Address)
	if poolAddr == (common.Address{}) {
		return common.Address{}, fmt.Errorf("aerodrome pool missing")
	}
	pr.cache.Store(key, poolAddr)
	return poolAddr, nil
}

func (pr *PoolRegistry) GetPool(tokenA, tokenB common.Address, fee uint32, stable bool) (common.Address, bool) {
	t0, t1 := sortTokens(tokenA, tokenB)
	key := PoolKey{TokenA: t0, TokenB: t1, Fee: fee, Stable: stable}
	val, ok := pr.cache.Load(key)
	if !ok {
		return common.Address{}, false
	}
	return val.(common.Address), true
}

func (pr *PoolRegistry) GetAllPools() []common.Address {
	var pools []common.Address
	pr.cache.Range(func(key, value interface{}) bool {
		pools = append(pools, value.(common.Address))
		return true
	})
	return pools
}

func (pr *PoolRegistry) Close() {
	if pr.client != nil { pr.client.Close() }
}
