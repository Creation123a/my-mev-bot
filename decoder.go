// Package ingestion provides zero-allocation parsing of Flashblock swap events.
package ingestion

import (
	"bytes"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"my-mev-bot/types"
)

// =============================================================================
// Pre-computed event signature topics (Keccak256 hashes)
// =============================================================================

var (
	// UniswapV2SwapTopic is the event signature for Swap(address,uint256,uint256,uint256,uint256,address)
	// Used by Uniswap V2, Aerodrome V2, AlienBase V2, etc.
	UniswapV2SwapTopic = common.HexToHash("0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822")

	// UniswapV3SwapTopic is the event signature for Swap(address,address,int256,int256,uint160,uint128,int24)
	// Used by Uniswap V3 and PancakeSwap V3.
	// Correct topic hash (verified against official Uniswap V3 source)
	UniswapV3SwapTopic = common.HexToHash("0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67")

	// tt256 = 2^256 (used for two's complement conversion of negative int256)
	tt256 = new(big.Int).Lsh(big.NewInt(1), 256)
)

// =============================================================================
// Decoder state – zero-allocation parser with reusable big.Int fields
// =============================================================================

// Decoder parses raw Flashblock log events. It contains mutable parsing state
// and must be used by a single goroutine at a time. Concurrent calls to
// ParseSwapLog on the same instance are unsupported.
type Decoder struct {
	scratch [64]byte // reusable scratch space for hex parsing (enough for 32-byte words)

	// Reusable big.Int fields for V2 swap parsing (avoid allocations in hot path)
	amount0In  big.Int
	amount1In  big.Int
	amount0Out big.Int
	amount1Out big.Int

	// Reusable big.Int fields for V3 swap parsing
	amount0 big.Int
	amount1 big.Int
	tmpSqrt big.Int
	tmpLiq  big.Int
}

// NewDecoder creates a new decoder.
func NewDecoder() *Decoder {
	return &Decoder{}
}

// =============================================================================
// Main parsing function
// =============================================================================

// ParseSwapLog parses a raw Flashblock log event into the provided SwapLog struct.
// It mutates outLog directly to avoid heap allocations. Returns nil on success.
func (d *Decoder) ParseSwapLog(rawLog []byte, outLog *types.SwapLog) error {
	outLog.Reset()

	topicHash, err := d.extractTopic0(rawLog)
	if err != nil {
		return fmt.Errorf("extract topic0: %w", err)
	}

	if bytes.Equal(topicHash.Bytes(), UniswapV2SwapTopic.Bytes()) {
		return d.decodeV2Swap(rawLog, outLog)
	}
	if bytes.Equal(topicHash.Bytes(), UniswapV3SwapTopic.Bytes()) {
		return d.decodeV3Swap(rawLog, outLog)
	}
	return fmt.Errorf("unknown event signature: %x", topicHash)
}

// =============================================================================
// V2 Swap Decoder
// =============================================================================

func (d *Decoder) decodeV2Swap(rawLog []byte, outLog *types.SwapLog) error {
	pool, err := d.extractAddress(rawLog)
	if err != nil {
		return err
	}
	outLog.Address = pool

	txHash, err := d.extractTxHash(rawLog)
	if err != nil {
		return err
	}
	outLog.TxHash = txHash

	blockNum, err := d.extractBlockNumber(rawLog)
	if err != nil {
		return err
	}
	outLog.BlockNumber = blockNum

	// Set timestamp to receive time (for latency tracking).
	outLog.Timestamp = time.Now()

	// Extract transaction index if present.
	if txIndex, err := d.extractTxIndex(rawLog); err == nil {
		outLog.TxIndex = uint(txIndex)
	} else {
		outLog.TxIndex = 0
	}

	// For V2, topics[1] and [2] are sender/recipient wallet addresses – NOT token addresses.
	// We will infer TokenIn/TokenOut from the amounts below.
	outLog.TokenIn = common.Address{}
	outLog.TokenOut = common.Address{}

	// Decode amounts directly into the reusable big.Int fields.
	if err := d.extractV2Amounts(rawLog); err != nil {
		return err
	}

	// Determine which side is in/out and assign to outLog.
	// The event structure: amount0In, amount1In, amount0Out, amount1Out.
	// In a typical pool, either amount0In and amount1Out are positive (swap token0 -> token1)
	// or amount1In and amount0Out are positive (swap token1 -> token0).
	if d.amount0In.Sign() > 0 && d.amount1Out.Sign() > 0 {
		// token0 is input, token1 is output.
		outLog.AmountIn.Set(&d.amount0In)
		outLog.AmountOut.Set(&d.amount1Out)
		// Note: we cannot set TokenIn/TokenOut until we know the pool's token order.
		// We'll leave them zero and let matrix infer from pool state.
	} else if d.amount1In.Sign() > 0 && d.amount0Out.Sign() > 0 {
		outLog.AmountIn.Set(&d.amount1In)
		outLog.AmountOut.Set(&d.amount0Out)
	} else {
		return fmt.Errorf("no valid input/output amounts in V2 swap")
	}
	outLog.AmountInFloat = float64FromBig(outLog.AmountIn)
	outLog.AmountOutFloat = float64FromBig(outLog.AmountOut)
	return nil
}

// extractV2Amounts decodes the four amount fields from the data field into the decoder's
// reusable big.Int fields.
func (d *Decoder) extractV2Amounts(rawLog []byte) error {
	data, err := d.extractData(rawLog)
	if err != nil {
		return err
	}
	if len(data) < 256 {
		return fmt.Errorf("data hex too short for V2 swap")
	}

	// amount0In
	if err := d.hexToBytes(d.scratch[:32], data[0:64]); err != nil {
		return err
	}
	d.amount0In.SetBytes(d.scratch[:32])

	// amount1In
	if err := d.hexToBytes(d.scratch[:32], data[64:128]); err != nil {
		return err
	}
	d.amount1In.SetBytes(d.scratch[:32])

	// amount0Out
	if err := d.hexToBytes(d.scratch[:32], data[128:192]); err != nil {
		return err
	}
	d.amount0Out.SetBytes(d.scratch[:32])

	// amount1Out
	if err := d.hexToBytes(d.scratch[:32], data[192:256]); err != nil {
		return err
	}
	d.amount1Out.SetBytes(d.scratch[:32])

	return nil
}

// =============================================================================
// V3 Swap Decoder
// =============================================================================

func (d *Decoder) decodeV3Swap(rawLog []byte, outLog *types.SwapLog) error {
	pool, err := d.extractAddress(rawLog)
	if err != nil {
		return err
	}
	outLog.Address = pool

	txHash, err := d.extractTxHash(rawLog)
	if err != nil {
		return err
	}
	outLog.TxHash = txHash

	blockNum, err := d.extractBlockNumber(rawLog)
	if err != nil {
		return err
	}
	outLog.BlockNumber = blockNum

	// Set timestamp to receive time.
	outLog.Timestamp = time.Now()

	// Extract transaction index if present.
	if txIndex, err := d.extractTxIndex(rawLog); err == nil {
		outLog.TxIndex = uint(txIndex)
	} else {
		outLog.TxIndex = 0
	}

	// For V3, topics[1] and [2] are sender/recipient wallet addresses – NOT token addresses.
	// We'll set TokenIn/TokenOut based on the sign of amount0/amount1 after decoding.
	// But we need the pool's token order; we can't know it here without an RPC.
	// We'll leave them zero and let matrix infer from the price change, which is more reliable.
	outLog.TokenIn = common.Address{}
	outLog.TokenOut = common.Address{}

	// Decode amounts (int256) and V3 state (sqrtPriceX96, liquidity, tick).
	if err := d.extractV3State(rawLog, outLog); err != nil {
		return err
	}

	// Determine which side is in/out based on the sign of amount0 and amount1.
	// In Uniswap V3, exactly one of amount0, amount1 is positive (input) and the other negative (output).
	if d.amount0.Sign() > 0 && d.amount1.Sign() < 0 {
		// token0 is input, token1 is output.
		outLog.AmountIn.Set(&d.amount0)
		d.amount1.Neg(&d.amount1) // make positive
		outLog.AmountOut.Set(&d.amount1)
	} else if d.amount1.Sign() > 0 && d.amount0.Sign() < 0 {
		// token1 is input, token0 is output.
		outLog.AmountIn.Set(&d.amount1)
		d.amount0.Neg(&d.amount0)
		outLog.AmountOut.Set(&d.amount0)
	} else {
		// A valid V3 swap always has exactly one positive and one negative amount.
		// Reject the event; the old fallback would guess incorrectly.
		return fmt.Errorf("ambiguous V3 swap amounts: amount0 sign %d, amount1 sign %d",
			d.amount0.Sign(), d.amount1.Sign())
	}
	outLog.AmountInFloat = float64FromBig(outLog.AmountIn)
	outLog.AmountOutFloat = float64FromBig(outLog.AmountOut)
	return nil
}

// extractV3State decodes amount0, amount1, sqrtPriceX96, liquidity, and tick
// from the V3 swap event data into the decoder's fields and outLog.
func (d *Decoder) extractV3State(rawLog []byte, outLog *types.SwapLog) error {
	data, err := d.extractData(rawLog)
	if err != nil {
		return err
	}
	// Minimum length: amount0 (32 bytes) + amount1 (32) + sqrtPriceX96 (32) + liquidity (32) + tick (32) = 160 bytes = 320 hex chars
	if len(data) < 320 {
		return fmt.Errorf("V3 data too short (expected >=320 hex chars, got %d)", len(data))
	}

	// amount0 (signed 256-bit) at offset 0
	if err := d.hexToBytes(d.scratch[:32], data[0:64]); err != nil {
		return err
	}
	d.amount0.SetBytes(d.scratch[:32])
	if isNegativeHex(data[0]) {
		d.amount0.Sub(&d.amount0, tt256)
	}

	// amount1 (signed 256-bit) at offset 64
	if err := d.hexToBytes(d.scratch[:32], data[64:128]); err != nil {
		return err
	}
	d.amount1.SetBytes(d.scratch[:32])
	if isNegativeHex(data[64]) {
		d.amount1.Sub(&d.amount1, tt256)
	}

	// sqrtPriceX96 (uint160) at offset 128 – decode as 32-byte word (padded)
	if err := d.hexToBytes(d.scratch[:32], data[128:192]); err != nil {
		return err
	}
	d.tmpSqrt.SetBytes(d.scratch[:32])
	outLog.SqrtPriceX96.Set(&d.tmpSqrt)
	outLog.SqrtPriceX96Float = float64FromBig(outLog.SqrtPriceX96)

	// liquidity (uint128) at offset 192 – decode as 32-byte word
	if err := d.hexToBytes(d.scratch[:32], data[192:256]); err != nil {
		return err
	}
	d.tmpLiq.SetBytes(d.scratch[:32])
	outLog.Liquidity.Set(&d.tmpLiq)
	outLog.LiquidityFloat = float64FromBig(outLog.Liquidity)

	// tick (int24) at offset 256 – decode the full 32-byte word, extract last 3 bytes, sign-extend to int32
	if err := d.hexToBytes(d.scratch[:32], data[256:320]); err != nil {
		return err
	}
	// The tick is stored in the last 3 bytes (bytes 29-31) of the 32-byte word
	var tickVal int32
	b := d.scratch[:32]
	tickVal = int32(b[29])<<16 | int32(b[30])<<8 | int32(b[31])
	// Sign extend if the 24-bit value is negative (bit 23 set)
	if tickVal&0x800000 != 0 {
		// Use subtraction to avoid int32 overflow (0xFF000000 is not representable in int32).
		tickVal -= 0x1000000
	}
	outLog.Tick = tickVal

	return nil
}

// isNegativeHex returns true if the first hex character (nibble) indicates that the
// most significant bit (bit 255 of a 32‑byte word) is set (i.e., the hex digit is >= 8).
func isNegativeHex(c byte) bool {
	return (c >= '8' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// float64FromBig converts a big.Int to float64 (approximation, zero‑allocation).
func float64FromBig(v *big.Int) float64 {
	if v == nil {
		return 0
	}
	f, _ := new(big.Float).SetInt(v).Float64()
	return f
}

// =============================================================================
// Low-level extraction helpers (zero-allocation)
// =============================================================================

// extractTopic0 returns the first topic (event signature) from the raw log.
func (d *Decoder) extractTopic0(rawLog []byte) (common.Hash, error) {
	idx := bytes.Index(rawLog, []byte(`"topics":`))
	if idx == -1 {
		return common.Hash{}, fmt.Errorf("topics field not found")
	}
	idx += len(`"topics":`)
	idx = skipWhitespace(rawLog, idx)
	if idx >= len(rawLog) || rawLog[idx] != '[' {
		return common.Hash{}, fmt.Errorf("topics array start not found")
	}
	idx++ // skip '['
	idx = skipWhitespace(rawLog, idx)
	if idx >= len(rawLog) || rawLog[idx] != '"' {
		return common.Hash{}, fmt.Errorf("topic0 not quoted")
	}
	idx++ // skip opening quote
	start := idx
	for idx < len(rawLog) && rawLog[idx] != '"' {
		idx++
	}
	if idx >= len(rawLog) {
		return common.Hash{}, fmt.Errorf("topic0 closing quote not found")
	}
	topicHex := rawLog[start:idx]
	return common.HexToHash(string(topicHex)), nil
}

// extractTopic returns the nth topic (0‑indexed) as an address.
func (d *Decoder) extractTopic(rawLog []byte, index int) (common.Address, error) {
	idx := bytes.Index(rawLog, []byte(`"topics":`))
	if idx == -1 {
		return common.Address{}, fmt.Errorf("topics field not found")
	}
	idx += len(`"topics":`)
	idx = skipWhitespace(rawLog, idx)
	if idx >= len(rawLog) || rawLog[idx] != '[' {
		return common.Address{}, fmt.Errorf("topics array start not found")
	}
	idx++ // skip '['

	// Skip to the correct topic.
	for i := 0; i < index; i++ {
		// Find next comma or array end.
		for idx < len(rawLog) && rawLog[idx] != ',' {
			idx++
		}
		if idx >= len(rawLog) {
			return common.Address{}, fmt.Errorf("topic %d not found", index)
		}
		idx++ // skip comma
		idx = skipWhitespace(rawLog, idx)
	}
	if idx >= len(rawLog) || rawLog[idx] != '"' {
		return common.Address{}, fmt.Errorf("topic %d not quoted", index)
	}
	idx++ // skip opening quote
	start := idx
	for idx < len(rawLog) && rawLog[idx] != '"' {
		idx++
	}
	if idx >= len(rawLog) {
		return common.Address{}, fmt.Errorf("topic %d closing quote not found", index)
	}
	addrHex := rawLog[start:idx]
	return common.HexToAddress(string(addrHex)), nil
}

// extractAddress extracts the 'address' field from a log entry.
func (d *Decoder) extractAddress(rawLog []byte) (common.Address, error) {
	key := []byte(`"address":`)
	idx := bytes.Index(rawLog, key)
	if idx == -1 {
		return common.Address{}, fmt.Errorf("address field not found")
	}
	idx += len(key)
	idx = skipWhitespace(rawLog, idx)
	if idx >= len(rawLog) || rawLog[idx] != '"' {
		return common.Address{}, fmt.Errorf("address value not quoted")
	}
	idx++ // skip opening quote
	start := idx
	for idx < len(rawLog) && rawLog[idx] != '"' {
		idx++
	}
	if idx >= len(rawLog) {
		return common.Address{}, fmt.Errorf("address closing quote not found")
	}
	addrHex := rawLog[start:idx]
	return common.HexToAddress(string(addrHex)), nil
}

// extractTxHash extracts the transaction hash from a log entry.
func (d *Decoder) extractTxHash(rawLog []byte) (common.Hash, error) {
	key := []byte(`"transactionHash":`)
	idx := bytes.Index(rawLog, key)
	if idx == -1 {
		return common.Hash{}, fmt.Errorf("transactionHash not found")
	}
	idx += len(key)
	idx = skipWhitespace(rawLog, idx)
	if idx >= len(rawLog) || rawLog[idx] != '"' {
		return common.Hash{}, fmt.Errorf("transactionHash not quoted")
	}
	idx++ // skip opening quote
	start := idx
	for idx < len(rawLog) && rawLog[idx] != '"' {
		idx++
	}
	if idx >= len(rawLog) {
		return common.Hash{}, fmt.Errorf("transactionHash closing quote not found")
	}
	txHex := rawLog[start:idx]
	return common.HexToHash(string(txHex)), nil
}

// extractBlockNumber extracts the block number from a log entry.
func (d *Decoder) extractBlockNumber(rawLog []byte) (uint64, error) {
	key := []byte(`"blockNumber":`)
	idx := bytes.Index(rawLog, key)
	if idx == -1 {
		return 0, fmt.Errorf("blockNumber not found")
	}
	idx += len(key)
	idx = skipWhitespace(rawLog, idx)
	if idx >= len(rawLog) {
		return 0, fmt.Errorf("blockNumber value missing")
	}
	// It may be quoted or unquoted.
	if rawLog[idx] == '"' {
		idx++ // skip opening quote
		start := idx
		for idx < len(rawLog) && rawLog[idx] != '"' {
			idx++
		}
		if idx >= len(rawLog) {
			return 0, fmt.Errorf("blockNumber closing quote not found")
		}
		val := rawLog[start:idx]
		return parseUint64(val)
	}
	// Unquoted number (hex or decimal)
	start := idx
	for idx < len(rawLog) && (rawLog[idx] == 'x' || (rawLog[idx] >= '0' && rawLog[idx] <= '9') || (rawLog[idx] >= 'a' && rawLog[idx] <= 'f') || (rawLog[idx] >= 'A' && rawLog[idx] <= 'F')) {
		idx++
	}
	val := rawLog[start:idx]
	return parseUint64(val)
}

// extractTxIndex extracts the transaction index from a log entry.
func (d *Decoder) extractTxIndex(rawLog []byte) (uint64, error) {
	key := []byte(`"transactionIndex":`)
	idx := bytes.Index(rawLog, key)
	if idx == -1 {
		return 0, fmt.Errorf("transactionIndex not found")
	}
	idx += len(key)
	idx = skipWhitespace(rawLog, idx)
	if idx >= len(rawLog) {
		return 0, fmt.Errorf("transactionIndex value missing")
	}
	// It may be quoted or unquoted.
	if rawLog[idx] == '"' {
		idx++ // skip opening quote
		start := idx
		for idx < len(rawLog) && rawLog[idx] != '"' {
			idx++
		}
		if idx >= len(rawLog) {
			return 0, fmt.Errorf("transactionIndex closing quote not found")
		}
		val := rawLog[start:idx]
		return parseUint64(val)
	}
	// Unquoted number.
	start := idx
	for idx < len(rawLog) && (rawLog[idx] >= '0' && rawLog[idx] <= '9') {
		idx++
	}
	val := rawLog[start:idx]
	return parseUint64(val)
}

// extractData extracts the 'data' field hex string (without 0x prefix) from the log.
func (d *Decoder) extractData(rawLog []byte) ([]byte, error) {
	key := []byte(`"data":`)
	idx := bytes.Index(rawLog, key)
	if idx == -1 {
		return nil, fmt.Errorf("data field not found")
	}
	idx += len(key)
	idx = skipWhitespace(rawLog, idx)
	if idx >= len(rawLog) || rawLog[idx] != '"' {
		return nil, fmt.Errorf("data field not quoted")
	}
	idx++ // skip opening quote
	start := idx
	for idx < len(rawLog) && rawLog[idx] != '"' {
		idx++
	}
	if idx >= len(rawLog) {
		return nil, fmt.Errorf("data field closing quote not found")
	}
	dataHex := rawLog[start:idx]
	// If it has 0x prefix, remove it.
	if len(dataHex) >= 2 && dataHex[0] == '0' && (dataHex[1] == 'x' || dataHex[1] == 'X') {
		dataHex = dataHex[2:]
	}
	return dataHex, nil
}

// =============================================================================
// Core utility: zero‑allocation hex to bytes
// =============================================================================

// hexToBytes decodes a hex slice (without 0x prefix) into the destination slice.
// dest must have length at least len(hex)/2. It returns an error on invalid hex.
func (d *Decoder) hexToBytes(dest []byte, hex []byte) error {
	if len(hex)%2 != 0 {
		return fmt.Errorf("hex string has odd length")
	}
	if len(dest) < len(hex)/2 {
		return fmt.Errorf("dest too small")
	}
	for i := 0; i < len(hex); i += 2 {
		var high, low byte
		switch hex[i] {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			high = hex[i] - '0'
		case 'a', 'b', 'c', 'd', 'e', 'f':
			high = hex[i] - 'a' + 10
		case 'A', 'B', 'C', 'D', 'E', 'F':
			high = hex[i] - 'A' + 10
		default:
			return fmt.Errorf("invalid hex character %c", hex[i])
		}
		switch hex[i+1] {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			low = hex[i+1] - '0'
		case 'a', 'b', 'c', 'd', 'e', 'f':
			low = hex[i+1] - 'a' + 10
		case 'A', 'B', 'C', 'D', 'E', 'F':
			low = hex[i+1] - 'A' + 10
		default:
			return fmt.Errorf("invalid hex character %c", hex[i+1])
		}
		dest[i/2] = (high << 4) | low
	}
	return nil
}

// =============================================================================
// Additional helpers
// =============================================================================

// skipWhitespace advances the index past any whitespace characters.
func skipWhitespace(b []byte, i int) int {
	for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r') {
		i++
	}
	return i
}

// parseUint64 parses a byte slice as a uint64, handling hex (0x prefix) or decimal.
func parseUint64(b []byte) (uint64, error) {
	if len(b) == 0 {
		return 0, fmt.Errorf("empty number")
	}
	if len(b) >= 2 && (b[0] == '0' && (b[1] == 'x' || b[1] == 'X')) {
		return strconv.ParseUint(string(b[2:]), 16, 64)
	}
	return strconv.ParseUint(string(b), 10, 64)
}