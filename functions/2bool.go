package functions

import (
	"github.com/yjhatfdu/expr/types"
	"strconv"
)

func init() {
	toBool, _ := NewFunction("toBool")
	toBool.Overload([]types.BaseType{types.Int}, types.Int, func(vectors []types.INullableVector, env map[string]string) (vector types.INullableVector, e error) {
		output := &types.NullableBool{}
		input := vectors[0].(*types.NullableInt)
		output.Init(input.Length())
		output.IsScalaV = input.IsScalaV
		for i := range input.Values {
			if i > 0 {
				output.Values[i] = true
			} else {
				output.Values[i] = false
			}
		}
		output.IsNullArr = input.IsNullArr
		output.FilterArr = input.FilterArr
		return output, nil
	})
	toBool.Overload([]types.BaseType{types.Float}, types.Int, func(vectors []types.INullableVector, env map[string]string) (vector types.INullableVector, e error) {
		output := &types.NullableBool{}
		input := vectors[0].(*types.NullableFloat)
		output.Init(input.Length())
		output.IsScalaV = input.IsScalaV
		for i := range input.Values {
			if i > 0 {
				output.Values[i] = true
			} else {
				output.Values[i] = false
			}
		}
		output.IsNullArr = input.IsNullArr
		output.FilterArr = input.FilterArr
		return output, nil
	})
	toBool.Overload([]types.BaseType{types.Text}, types.Int, func(vectors []types.INullableVector, env map[string]string) (vector types.INullableVector, e error) {
		output := &types.NullableBool{}
		input := vectors[0].(*types.NullableText)
		//output.FilterArr = input.FilterArr
		return BroadCast1(vectors[0], output, func(i int) error {
			ret, err := strconv.ParseBool(input.Values[i])
			if err != nil {
				return err
			}
			output.Seti(i, ret)
			return nil
		})
	})
	toBool.Overload([]types.BaseType{types.Bool}, types.Int, func(vectors []types.INullableVector, env map[string]string) (vector types.INullableVector, e error) {
		return vectors[0], nil
	})
}