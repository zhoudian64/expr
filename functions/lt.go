package functions

import "expr/types"

func init() {
	addFunc, _ := NewFunction("lt")
	addFunc.Overload([]types.BaseType{types.Int, types.Int}, types.Bool, func(vectors []types.INullableVector) (types.INullableVector, error) {
		output := types.NullableBool{}
		left := vectors[0].(*types.NullableInt)
		right := vectors[1].(*types.NullableInt)
		return BroadCast2(vectors[0], vectors[1], &output, func(index, i, j int) error {
			output.Set(index, left.Values[i] < right.Values[j], false)
			return nil
		})
	})
	addFunc.Overload([]types.BaseType{types.Int, types.Float}, types.Bool, func(vectors []types.INullableVector) (vector types.INullableVector, e error) {
		output := types.NullableBool{}
		left := vectors[0].(*types.NullableInt)
		right := vectors[1].(*types.NullableFloat)
		return BroadCast2(vectors[0], vectors[1], &output, func(index, i, j int) error {
			output.Set(index, float64(left.Values[i]) < right.Values[j], false)
			return nil
		})
	})
	addFunc.Overload([]types.BaseType{types.Float, types.Int}, types.Bool, func(vectors []types.INullableVector) (vector types.INullableVector, e error) {
		output := types.NullableBool{}
		left := vectors[0].(*types.NullableFloat)
		right := vectors[1].(*types.NullableInt)
		return BroadCast2(vectors[0], vectors[1], &output, func(index, i, j int) error {
			output.Set(index, left.Values[i] < float64(right.Values[j]), false)
			return nil
		})
	})
	addFunc.Overload([]types.BaseType{types.Float, types.Float}, types.Bool, func(vectors []types.INullableVector) (vector types.INullableVector, e error) {
		output := types.NullableBool{}
		left := vectors[0].(*types.NullableFloat)
		right := vectors[1].(*types.NullableFloat)
		return BroadCast2(vectors[0], vectors[1], &output, func(index, i, j int) error {
			output.Set(index, left.Values[i] < right.Values[j], false)
			return nil
		})
	})
	addFunc.Overload([]types.BaseType{types.Text, types.Text}, types.Bool, func(vectors []types.INullableVector) (vector types.INullableVector, e error) {
		output := types.NullableBool{}
		left := vectors[0].(*types.NullableText)
		right := vectors[1].(*types.NullableText)
		return BroadCast2(vectors[0], vectors[1], &output, func(index, i, j int) error {
			output.Set(index, left.Values[i] < right.Values[j], false)
			return nil
		})
	})
}

func init() {
	addFunc, _ := NewFunction("lte")
	addFunc.Overload([]types.BaseType{types.Int, types.Int}, types.Bool, func(vectors []types.INullableVector) (types.INullableVector, error) {
		output := types.NullableBool{}
		left := vectors[0].(*types.NullableInt)
		right := vectors[1].(*types.NullableInt)
		return BroadCast2(vectors[0], vectors[1], &output, func(index, i, j int) error {
			output.Set(index, left.Values[i] <= right.Values[j], false)
			return nil
		})
	})
	addFunc.Overload([]types.BaseType{types.Int, types.Float}, types.Bool, func(vectors []types.INullableVector) (vector types.INullableVector, e error) {
		output := types.NullableBool{}
		left := vectors[0].(*types.NullableInt)
		right := vectors[1].(*types.NullableFloat)
		return BroadCast2(vectors[0], vectors[1], &output, func(index, i, j int) error {
			output.Set(index, float64(left.Values[i]) <= right.Values[j], false)
			return nil
		})
	})
	addFunc.Overload([]types.BaseType{types.Float, types.Int}, types.Bool, func(vectors []types.INullableVector) (vector types.INullableVector, e error) {
		output := types.NullableBool{}
		left := vectors[0].(*types.NullableFloat)
		right := vectors[1].(*types.NullableInt)
		return BroadCast2(vectors[0], vectors[1], &output, func(index, i, j int) error {
			output.Set(index, left.Values[i] <= float64(right.Values[j]), false)
			return nil
		})
	})
	addFunc.Overload([]types.BaseType{types.Float, types.Float}, types.Bool, func(vectors []types.INullableVector) (vector types.INullableVector, e error) {
		output := types.NullableBool{}
		left := vectors[0].(*types.NullableFloat)
		right := vectors[1].(*types.NullableFloat)
		return BroadCast2(vectors[0], vectors[1], &output, func(index, i, j int) error {
			output.Set(index, left.Values[i] <= right.Values[j], false)
			return nil
		})
	})
	addFunc.Overload([]types.BaseType{types.Text, types.Text}, types.Bool, func(vectors []types.INullableVector) (vector types.INullableVector, e error) {
		output := types.NullableBool{}
		left := vectors[0].(*types.NullableText)
		right := vectors[1].(*types.NullableText)
		return BroadCast2(vectors[0], vectors[1], &output, func(index, i, j int) error {
			output.Set(index, left.Values[i] <= right.Values[j], false)
			return nil
		})
	})
}
