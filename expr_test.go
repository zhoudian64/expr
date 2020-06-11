package expr

import (
	"expr/types"
	"testing"
)

func TestExpr(t *testing.T) {
	code := "1+1"
	p, err := Compile(code, nil)
	if err != nil {
		panic(err)
	}
	ret, err := p.Run(nil)
	if err != nil {
		panic(err)
	}
	t.Log(types.ToString(ret))
}
func TestExpr2(t *testing.T) {
	code := "$1 or $1 and $1"
	p, err := Compile(code, []types.BaseType{types.Int})
	if err != nil {
		panic(err)
	}
	ret, err := p.Run([]types.INullableVector{types.BuildValue(types.Int, 1, 2, 3, 4, 5, 6, nil, 8, 9, 10, )})
	if err != nil {
		panic(err)
	}
	t.Log(types.ToString(ret))
}

func TestExpr3(t *testing.T) {
	code := "add($1,$1)"
	p, err := Compile(code, []types.BaseType{types.Int})
	if err != nil {
		panic(err)
	}
	ret, err := p.Run([]types.INullableVector{types.BuildValue(types.Int, 1, 2, 3, 4, 5, 6, nil, 8, 9, 10, )})
	if err != nil {
		panic(err)
	}
	t.Log(types.ToString(ret))
}

func TestExpr4(t *testing.T) {
	code := "coalesce($1,$2,1)"
	p, err := Compile(code, []types.BaseType{types.Int,types.Int})
	if err != nil {
		panic(err)
	}
	ret, err := p.Run([]types.INullableVector{types.BuildValue(types.Int, nil, 2, 3, 4, 5, 6, nil, 8, 9, 10, ),types.BuildValue(types.Int, 10, 2, 3, 4, 5, 6, nil, 8, 9, 10, )})
	if err != nil {
		panic(err)
	}
	t.Log(types.ToString(ret))
}

func BenchmarkExpr(b *testing.B) {
	code := "$1 > 1"
	p, err := Compile(code, []types.BaseType{types.Int})
	if err != nil {
		panic(err)
	}
	input := []types.INullableVector{types.BuildValue(types.Int, 1, 2, 3, 4, 5, 6, nil, 8, 9, 10, 1, 2, 3, 4, 5, 6, nil, 8, 9, 10, 1, 2, 3, 4, 5, 6, nil, 8, 9, 10, 1, 2, 3, 4, 5, 6, nil, 8, 9, 10, 1, 2, 3, 4, 5, 6, nil, 8, 9, 10, 1, 2, 3, 4, 5, 6, nil, 8, 9, 10, 1, 2, 3, 4, 5, 6, nil, 8, 9, 10, 1, 2, 3, 4, 5, 6, nil, 8, 9, 10, 1, 2, 3, 4, 5, 6, nil, 8, 9, 10, 1, 2, 3, 4, 5, 6, nil, 8, 9, 10, )}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = p.Run(input)

	}
}