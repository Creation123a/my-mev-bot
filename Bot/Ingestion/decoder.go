// Package ingestion provides zero-allocation parsing of Flashblock swap events.
package ingestion

import (
	"bytes"
	"fmt"
	"math/big"
	"strconv"
	"time"
	"unsafe"

	"github.com/buger/jsonparser"
	"github.com/ethereum/go-ethereum/common"

	"my-mev-bot/Bot/Types"
)

// =============================================================================
// Pre-computed event signature topics (Keccak256 hashes)
// =============================================================================

var (
	UniswapV2SwapTopic = common.HexToHash("0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822")
	UniswapV3SwapTopic = common.HexToHash("0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67")
	tt256              = new(big.Int).Lsh(big.NewInt(1), 256)
)

// =============================================================================
// Decoder state – reusable big.Int fields for zero‑allocation amount parsing
// =============================================================================

type Decoder struct {
	scratch [64]byte

	amount0In  big.Int
	amount1In  big.Int
	amount0Out big.Int
	amount1Out big.Int

	amount0  big.Int
	amount1  big.Int
	tmpSqrt  big.Int
	tmpLiq   big.Int
}

func NewDecoder() *Decoder {
	return &Decoder{}
}

// =============================================================================
// Zero‑allocation JSON parser using jsonparser + unsafe
// =============================================================================

// FastString converts a byte slice to a string with zero allocations.
// WARNING: The string is valid only as long as the underlying byte buffer is alive.
func FastString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return *(*string)(unsafe.Pointer(&b))
}

// hexToAddressBytes decodes 0x‑prefixed hex into a 20‑byte slice with bounds checks.
func hexToAddressBytes(hex []byte, dest []byte) error {
	if len(hex) < 42 || hex[0] != '0' || (hex[1] != 'x' && hex[1] != 'X') {
		return fmt.Errorf("invalid hex address")
	}
	src := hex[2:]
	if len(src) != 40 {
		return fmt.Errorf("address length must be 40 hex chars")
	}
	for i := 0; i < 20; i++ {
		high := fromHexChar(src[i*2])
		low := fromHexChar(src[i*2+1])
		if high == 0xff || low == 0xff {
			return fmt.Errorf("invalid hex char")
		}
		dest[i] = (high << 4) | low
	}
	return nil
}

// hexToHashBytes decodes 0x‑prefixed hex into a 32‑byte slice with bounds checks.
func hexToHashBytes(hex []byte, dest []byte) error {
	if len(hex) < 66 || hex[0] != '0' || (hex[1] != 'x' && hex[1] != 'X') {
		return fmt.Errorf("invalid hex hash")
	}
	src := hex[2:]
	if len(src) != 64 {
		return fmt.Errorf("hash length must be 64 hex chars")
	}
	for i := 0; i < 32; i++ {
		high := fromHexChar(src[i*2])
		low := fromHexChar(src[i*2+1])
		if high == 0xff || low == 0xff {
			return fmt.Errorf("invalid hex char")
		}
		dest[i] = (high << 4) | low
	}
	return nil
}

func fromHexChar(c byte) byte {
	if c >= '0' && c <= '9' {
		return c - '0'
	}
	if c >= 'a' && c <= 'f' {
		return c - 'a' + 10
	}
	if c >= 'A' && c <= 'F' {
		return c - 'A' + 10
	}
	return 0xff
}

// parseSwapLogZeroAlloc fills a SwapLog from raw JSON log data using jsonparser.
// It reuses the Decoder's hex decoding methods and big.Int fields.
func parseSwapLogZeroAlloc(data []byte, out *types.SwapLog, decoder *Decoder) error {
	if len(data) < 10 {
		return fmt.Errorf("log data too short")
	}
	out.Reset()
	out.Timestamp = time.Now()

	// --- address ---
	addrBytes, _, _, err := jsonparser.Get(data, "address")
	if err != nil || len(addrBytes) < 42 {
		return fmt.Errorf("address missing or invalid")
	}
	if err := hexToAddressBytes(addrBytes, out.Address[:]); err != nil {
		return err
	}

	// --- blockNumber ---
	blockNum, err := jsonparser.GetInt(data, "blockNumber")
	if err != nil {
		return err
	}
	out.BlockNumber = uint64(blockNum)

	// --- transactionHash ---
	txHashBytes, _, _, err := jsonparser.Get(data, "transactionHash")
	if err != nil || len(txHashBytes) < 66 {
		return fmt.Errorf("txHash missing")
	}
	if err := hexToHashBytes(txHashBytes, out.TxHash[:]); err != nil {
		return err
	}

	// --- transactionIndex (optional) ---
	if txIndex, err := jsonparser.GetInt(data, "transactionIndex"); err == nil {
		out.TxIndex = uint(txIndex)
	}

	// --- topics (array) ---
	var topics [4]common.Hash
	topicCount := 0
	_, err = jsonparser.ArrayEach(data, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		if topicCount >= 4 || err != nil {
			return
		}
		if len(value) < 66 {
			err = fmt.Errorf("topic too short")
			return
		}
		var h common.Hash
		if e := hexToHashBytes(value, h[:]); e != nil {
			err = e
			return
		}
		topics[topicCount] = h
		topicCount++
	}, "topics")
	if err != nil {
		return err
	}
	// Reuse backing array of out.Topics to avoid allocation
	out.Topics = out.Topics[:0]
	out.Topics = append(out.Topics, topics[:topicCount]...)

	if topicCount == 0 {
		return fmt.Errorf("no topics found")
	}

	// --- data (hex string) ---
	dataHex, _, _, err := jsonparser.Get(data, "data")
	if err != nil || len(dataHex) < 2 {
		return fmt.Errorf("data missing")
	}
	// Remove 0x prefix if present.
	if len(dataHex) >= 2 && dataHex[0] == '0' && (dataHex[1] == 'x' || dataHex[1] == 'X') {
		dataHex = dataHex[2:]
	}

	// Determine event type from topic0.
	topic0 := topics[0]
	if bytes.Equal(topic0.Bytes(), UniswapV2SwapTopic.Bytes()) {
		return decoder.decodeV2SwapFromData(dataHex, out)
	}
	if bytes.Equal(topic0.Bytes(), UniswapV3SwapTopic.Bytes()) {
		return decoder.decodeV3SwapFromData(dataHex, out)
	}
	return fmt.Errorf("unknown event signature: %x", topic0)
}

// =============================================================================
// Internal decoders that work directly on hex data (no JSON scanning)
// =============================================================================

// decodeV2SwapFromData decodes V2 amounts from the data hex string.
func (d *Decoder) decodeV2SwapFromData(dataHex []byte, out *types.SwapLog) error {
	if len(dataHex) < 256 {
		return fmt.Errorf("V2 data hex too short (need 256 chars, got %d)", len(dataHex))
	}
	// Decode amounts directly into d.* fields.
	if err := d.hexToBytes(d.scratch[:32], dataHex[0:64]); err != nil {
		return err
	}
	d.amount0In.SetBytes(d.scratch[:32])
	if err := d.hexToBytes(d.scratch[:32], dataHex[64:128]); err != nil {
		return err
	}
	d.amount1In.SetBytes(d.scratch[:32])
	if err := d.hexToBytes(d.scratch[:32], dataHex[128:192]); err != nil {
		return err
	}
	d.amount0Out.SetBytes(d.scratch[:32])
	if err := d.hexToBytes(d.scratch[:32], dataHex[192:256]); err != nil {
		return err
	}
	d.amount1Out.SetBytes(d.scratch[:32])

	// Determine direction.
	if d.amount0In.Sign() > 0 && d.amount1Out.Sign() > 0 {
		out.AmountIn.Set(&d.amount0In)
		out.AmountOut.Set(&d.amount1Out)
	} else if d.amount1In.Sign() > 0 && d.amount0Out.Sign() > 0 {
		out.AmountIn.Set(&d.amount1In)
		out.AmountOut.Set(&d.amount0Out)
	} else {
		return fmt.Errorf("no valid V2 swap amounts")
	}
	out.AmountInFloat = float64FromBig(out.AmountIn)
	out.AmountOutFloat = float64FromBig(out.AmountOut)
	return nil
}

// decodeV3SwapFromData decodes V3 amounts and state from the data hex string.
func (d *Decoder) decodeV3SwapFromData(dataHex []byte, out *types.SwapLog) error {
	if len(dataHex) < 320 {
		return fmt.Errorf("V3 data hex too short (need 320 chars, got %d)", len(dataHex))
	}
	// amount0 (signed)
	if err := d.hexToBytes(d.scratch[:32], dataHex[0:64]); err != nil {
		return err
	}
	d.amount0.SetBytes(d.scratch[:32])
	if isNegativeHex(dataHex[0]) {
		d.amount0.Sub(&d.amount0, tt256)
	}
	// amount1 (signed)
	if err := d.hexToBytes(d.scratch[:32], dataHex[64:128]); err != nil {
		return err
	}
	d.amount1.SetBytes(d.scratch[:32])
	if isNegativeHex(dataHex[64]) {
		d.amount1.Sub(&d.amount1, tt256)
	}
	// sqrtPriceX96
	if err := d.hexToBytes(d.scratch[:32], dataHex[128:192]); err != nil {
		return err
	}
	d.tmpSqrt.SetBytes(d.scratch[:32])
	out.SqrtPriceX96.Set(&d.tmpSqrt)
	out.SqrtPriceX96Float = float64FromBig(out.SqrtPriceX96)
	// liquidity
	if err := d.hexToBytes(d.scratch[:32], dataHex[192:256]); err != nil {
		return err
	}
	d.tmpLiq.SetBytes(d.scratch[:32])
	out.Liquidity.Set(&d.tmpLiq)
	out.LiquidityFloat = float64FromBig(out.Liquidity)
	// tick (int24)
	if err := d.hexToBytes(d.scratch[:32], dataHex[256:320]); err != nil {
		return err
	}
	b := d.scratch[:32]
	tickVal := int32(b[29])<<16 | int32(b[30])<<8 | int32(b[31])
	if tickVal&0x800000 != 0 {
		tickVal -= 0x1000000
	}
	out.Tick = tickVal

	// Determine input/output direction.
	if d.amount0.Sign() > 0 && d.amount1.Sign() < 0 {
		out.AmountIn.Set(&d.amount0)
		d.amount1.Neg(&d.amount1)
		out.AmountOut.Set(&d.amount1)
	} else if d.amount1.Sign() > 0 && d.amount0.Sign() < 0 {
		out.AmountIn.Set(&d.amount1)
		d.amount0.Neg(&d.amount0)
		out.AmountOut.Set(&d.amount0)
	} else {
		return fmt.Errorf("ambiguous V3 swap amounts")
	}
	out.AmountInFloat = float64FromBig(out.AmountIn)
	out.AmountOutFloat = float64FromBig(out.AmountOut)
	return nil
}

// isNegativeHex returns true if the first hex character indicates a negative signed value.
func isNegativeHex(c byte) bool {
	return (c >= '8' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// float64FromBig converts a big.Int to float64 (approximation).
func float64FromBig(v *big.Int) float64 {
	if v == nil {
		return 0
	}
	f, _ := new(big.Float).SetInt(v).Float64()
	return f
}

// =============================================================================
// Legacy parser – kept for compatibility (uses manual scanning)
// =============================================================================

// ParseSwapLog parses a raw Flashblock log event using manual scanning.
// It is a fallback for code that does not use the zero‑alloc path.
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
// Legacy V2 decoder – uses manual extraction and then reuses the hex decoder
// =============================================================================

func (d *Decoder) decodeV2Swap(rawLog []byte, outLog *types.SwapLog) error {
	// Extract address, txHash, blockNumber, txIndex
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

	if txIndex, err := d.extractTxIndex(rawLog); err == nil {
		outLog.TxIndex = uint(txIndex)
	}

	// Extract topics (at least topic0)
	// We'll extract topics[1] and [2] if present to preserve original behavior.
	// For V2, topics[1] and [2] are sender/recipient, but we don't need them.
	// We only need to set outLog.Topics for compatibility.
	var topics [4]common.Hash
	topics[0] = UniswapV2SwapTopic
	// Try to extract topic1 and topic2
	if t1, err := d.extractTopic(rawLog, 1); err == nil {
		topics[1] = t1
	}
	if t2, err := d.extractTopic(rawLog, 2); err == nil {
		topics[2] = t2
	}
	outLog.Topics = outLog.Topics[:0]
	outLog.Topics = append(outLog.Topics, topics[:3]...)

	// Extract data hex and delegate to the new decoder.
	dataHex, err := d.extractData(rawLog)
	if err != nil {
		return err
	}
	// dataHex already has 0x prefix removed by extractData.
	return d.decodeV2SwapFromData(dataHex, outLog)
}

// =============================================================================
// Legacy V3 decoder – similar to V2
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

	if txIndex, err := d.extractTxIndex(rawLog); err == nil {
		outLog.TxIndex = uint(txIndex)
	}

	var topics [4]common.Hash
	topics[0] = UniswapV3SwapTopic
	if t1, err := d.extractTopic(rawLog, 1); err == nil {
		topics[1] = t1
	}
	if t2, err := d.extractTopic(rawLog, 2); err == nil {
		topics[2] = t2
	}
	outLog.Topics = outLog.Topics[:0]
	outLog.Topics = append(outLog.Topics, topics[:3]...)

	dataHex, err := d.extractData(rawLog)
	if err != nil {
		return err
	}
	return d.decodeV3SwapFromData(dataHex, outLog)
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
