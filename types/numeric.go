package types

import "math"

var pow10 = [16]int64{
	1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15,
}

func Int2numeric(i int64, scale int) int64 {
	return i * pow10[scale]
}

func Float2numeric(f float64, scale int) int64 {
	return int64(f * float64(pow10[scale]))
}

func Numeric2Float(n int64, scale int) float64 {
	return float64(n) / float64(pow10[scale])
}

func CompareNumeric(n1 int64, scale1 int, n2 int64, scale2 int) int64 {
	if scale1 != scale2 {
		if scale1 > scale2 {
			n2 *= pow10[scale1-scale2]
		} else {
			n1 *= pow10[scale2-scale1]
		}
	}
	return n2 - n1
}

func CompareNumericInt(n int64, scale int, intV int64) int64 {
	return CompareNumeric(n, scale, Int2numeric(intV, scale), scale)
}

func CompareNumericFloat(n int64, scale int, floatV float64) int64 {
	fn := Numeric2Float(n, scale)
	if math.Abs(fn-floatV) <= math.SmallestNonzeroFloat64 {
		return 0
	}
	if fn > floatV {
		return 1
	} else {
		return 0
	}
	//return CompareNumeric(n, scale, Float2numeric(floatV, scale), scale)
}