package functions

import (
	"errors"
	"fmt"
	"github.com/yjhatfdu/expr/types"
	"regexp"
	"strings"
	"unicode/utf8"
)

type regexpReplaceAllIndex struct {
	regexp *regexp.Regexp
	start  int
	end    int
}

func (s *regexpReplaceAllIndex) Init(consts []string, env map[string]string) error {
	return nil
}
func (s *regexpReplaceAllIndex) Handle(vectors []types.INullableVector, env map[string]string) (types.INullableVector, error) {
	if s.regexp == nil {
		r := vectors[1].Index(0).(string)
		var err error
		s.regexp, err = regexp.Compile(r)
		if err != nil {
			return nil, err
		}
	}

	var needSub bool
	var startIndex int
	var endIndex int

	if len(vectors) == 4 {
		startIndex = int(vectors[3].Index(0).(int64))
		needSub = true
	} else if len(vectors) == 5 {
		startIndex = int(vectors[3].Index(0).(int64))
		endIndex = int(vectors[4].Index(0).(int64))
		needSub = true
	} else {
		needSub = false
	}

	if needSub {
		if startIndex < 0 || endIndex < 0 {
			return nil, errors.New(fmt.Sprintf("regexpReplace 函数的 startIndex 与 endIndex 必须大于等于 0, 实际值 startIndex: %d, endIndex: %d", startIndex, endIndex))
		}

		if startIndex == endIndex {
			endIndex += 1
		}

		if startIndex > endIndex {
			return nil, errors.New(fmt.Sprintf("regexpReplace 函数的 startIndex 必须小于等于 endIndex, 实际值 startIndex: %d, endIndex: %d", startIndex, endIndex))
		}
	}

	replace := vectors[2].Index(0).(string)
	input := vectors[0].(*types.NullableText)
	out := &types.NullableText{}
	return BroadCast1(vectors[0], out, func(i int) error {
		var realEnd int
		v := input.Index(i).(string)
		if needSub {
			if len(v) <= endIndex {
				realEnd = len(v)
			} else {
				realEnd = endIndex
			}
		}

		if needSub {
			sub := string([]rune(v)[startIndex:realEnd])
			subReplaceStr := s.regexp.ReplaceAllString(sub, replace)
			out.Set(i, string([]rune(v)[0:startIndex])+subReplaceStr+string([]rune(v)[realEnd:]), false)
		} else {
			out.Set(i, s.regexp.ReplaceAllString(v, replace), false)
		}

		return nil
	})
}

type likeFunc struct {
	regexp *regexp.Regexp
	not    bool
}

func (s *likeFunc) Init(cons []string, env map[string]string) error {
	if len(cons) != 2 {
		return errors.New(fmt.Sprintf("like 函数仅接受两个参数，实际参数个数 %d", len(cons)))
	}

	pattern := cons[1]
	//if err != nil {
	//	return errors.New(fmt.Sprintf("未能成功解析 like 函数表达式 %s，异常信息 %s", cons[1], err))
	//}
	re, err := regexp.Compile(strings.ReplaceAll(strings.ReplaceAll(pattern, "%", ".*?"), "_", "."))
	//re, err := regexp.Compile(pattern)
	if err != nil {
		return errors.New(fmt.Sprintf("未能成功编译 like 函数表达式 %s，异常信息 %s", cons[1], err))
	}
	s.regexp = re
	return nil
}
func (s *likeFunc) Handle(vectors []types.INullableVector, env map[string]string) (types.INullableVector, error) {
	input := vectors[0].(*types.NullableText)
	output := &types.NullableBool{}
	return BroadCast1(input, output, func(i int) error {
		if s.not {
			output.Seti(i, !s.regexp.MatchString(input.Values[i]))
		} else {
			output.Seti(i, s.regexp.MatchString(input.Values[i]))
		}
		return nil
	})
}

func init() {
	trim, _ := NewFunction("trim")
	trim.Overload([]types.BaseType{types.Text}, types.Text, func(vectors []types.INullableVector, env map[string]string) (vector types.INullableVector, e error) {
		output := &types.NullableText{}
		input := vectors[0].(*types.NullableText)
		return BroadCast1(input, output, func(i int) error {
			output.Set(i, strings.TrimSpace(input.Values[i]), false)
			return nil
		})
	})
	trim.Overload([]types.BaseType{types.Text, types.TextS}, types.Text, func(vectors []types.INullableVector, env map[string]string) (vector types.INullableVector, e error) {
		output := &types.NullableText{}
		input := vectors[0].(*types.NullableText)
		t := vectors[1].(*types.NullableText).Index(0).(string)
		return BroadCast1(input, output, func(i int) error {
			var v = input.Values[i]
			for strings.HasPrefix(v, t) {
				v = strings.TrimLeft(v, t)
			}

			for strings.HasSuffix(v, t) {
				v = strings.TrimSuffix(v, t)
			}

			output.Seti(i, v)
			return nil
		})
	})

	length, _ := NewFunction("length")
	length.Overload([]types.BaseType{types.Text}, types.Int, func(vectors []types.INullableVector, env map[string]string) (vector types.INullableVector, e error) {
		input := vectors[0].(*types.NullableText)
		output := &types.NullableInt{}
		return BroadCast1(input, output, func(i int) error {
			output.Set(i, int64(utf8.RuneCountInString(input.Values[i])), false)
			return nil
		})
	})

	toLower, _ := NewFunction("toLower")
	toLower.Overload([]types.BaseType{types.Text}, types.Text, func(vectors []types.INullableVector, env map[string]string) (vector types.INullableVector, e error) {
		input := vectors[0].(*types.NullableText)
		output := &types.NullableText{}
		return BroadCast1(input, output, func(i int) error {
			output.Set(i, strings.ToLower(input.Values[i]), false)
			return nil
		})
	})

	toUpper, _ := NewFunction("toUpper")
	toUpper.Overload([]types.BaseType{types.Text}, types.Text, func(vectors []types.INullableVector, env map[string]string) (vector types.INullableVector, e error) {
		input := vectors[0].(*types.NullableText)
		output := &types.NullableText{}
		return BroadCast1(input, output, func(i int) error {
			output.Set(i, strings.ToUpper(input.Values[i]), false)
			return nil
		})
	})

	like, _ := NewFunction("like")
	like.OverloadHandler(
		[]types.BaseType{types.Text, types.TextS},
		types.Bool,
		func() IHandler { return &likeFunc{} },
	)
	notLike, _ := NewFunction("notLike")
	notLike.OverloadHandler(
		[]types.BaseType{types.Text, types.TextS},
		types.Bool,
		func() IHandler { return &likeFunc{not: true} },
	)
	//like.Overload([]types.BaseType{types.Text, types.Text}, types.Bool, func(vectors []types.INullableVector, env map[string]string) (vector types.INullableVector, e error) {
	//	output := &types.NullableBool{}
	//	input := vectors[0].(*types.NullableText)
	//	likeStr := vectors[1].(*types.NullableText).Index(0).(string)
	//	var rxp, err = regexp.Compile(strings.ReplaceAll(strings.ReplaceAll(likeStr, "%", ".*?"), "_", "."))
	//	if err != nil {
	//		return nil, errors.New(fmt.Sprintf("like函数匹配语法未能编译成正确的正则表达式，原始匹配语法 %s，编译异常信息 %s", likeStr, err.Error()))
	//	}
	//	return BroadCast1(input, output, func(i int) error {
	//		output.Seti(i, rxp.MatchString(input.Index(i).(string)))
	//		return nil
	//	})
	//})

	contains, _ := NewFunction("contains")
	contains.Overload([]types.BaseType{types.Text, types.Text}, types.Bool, func(vectors []types.INullableVector, env map[string]string) (vector types.INullableVector, e error) {
		input := vectors[0].(*types.NullableText)
		containsStr := vectors[1].(*types.NullableText).Index(0).(string)
		output := &types.NullableBool{}
		return BroadCast1(input, output, func(i int) error {
			output.Seti(i, strings.Contains(input.Index(i).(string), containsStr))
			return nil
		})
	})

	// similarTo
	// this function is for SQL Style `SIMILAR TO` operator
	// similar to LIKE, except interprets the pattern using SQL standard's
	// _ and % also can be used as wildcard characters.
	// [(comparable to . and .*) so we can just replace _ and % with . and .*]
	// according to https://postgresql.org/docs/13/functions-matching.html
	_similarTo, _ := NewFunction("similarTo")
	_similarTo.Overload(
		[]types.BaseType{types.Text, types.TextS},
		types.Bool,
		func(vectors []types.INullableVector, env map[string]string) (vector types.INullableVector, e error) {
			text := vectors[0].(*types.NullableText)
			pattern := vectors[1].(*types.NullableText).Index(0).(string)
			output := &types.NullableBool{}

			// copy from https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/regexp.c#L833
			// take a look.
			pattern = `^(?:` + pattern + `)$`
			var r, err = regexp.Compile(strings.ReplaceAll(strings.ReplaceAll(pattern, "%", ".*"), "_", "."))
			if err != nil {
				return nil, errors.New(fmt.Sprintf("SimilarTo函数匹配语法未能编译成正确的正则表达式, %v %v", pattern, err.Error()))
			}

			return BroadCast1(text, output, func(i int) error {
				output.Seti(i, r.MatchString(text.Index(i).(string)))
				return nil
			})
		})

	_notSimilarTo, _ := NewFunction("notSimilarTo")
	_notSimilarTo.Overload(
		[]types.BaseType{types.Text, types.TextS},
		types.Bool,
		func(vectors []types.INullableVector, env map[string]string) (vector types.INullableVector, e error) {
			text := vectors[0].(*types.NullableText)
			pattern := vectors[1].(*types.NullableText).Index(0).(string)
			output := &types.NullableBool{}

			pattern = `^(?:` + pattern + `)$`
			var r, err = regexp.Compile(strings.ReplaceAll(strings.ReplaceAll(pattern, "%", ".*"), "_", "."))
			if err != nil {
				return nil, errors.New(fmt.Sprintf("SimilarTo函数匹配语法未能编译成正确的正则表达式, %v %v", pattern, err.Error()))
			}

			return BroadCast1(text, output, func(i int) error {
				output.Seti(i, !r.MatchString(text.Index(i).(string)))
				return nil
			})
		})

	replaceAll, _ := NewFunction("regexpReplace")
	replaceAll.OverloadHandler(
		[]types.BaseType{types.Text, types.TextS, types.TextS},
		types.Text,
		func() IHandler { return &regexpReplaceAllIndex{} },
	)
	replaceAll.OverloadHandler(
		[]types.BaseType{types.Text, types.TextS, types.TextS, types.IntS},
		types.Text,
		func() IHandler { return &regexpReplaceAllIndex{} },
	)
	replaceAll.OverloadHandler(
		[]types.BaseType{types.Text, types.TextS, types.TextS, types.IntS, types.IntS},
		types.Text,
		func() IHandler { return &regexpReplaceAllIndex{} },
	)

	regexpMatch, _ := NewFunction("regexpMatch")
	regexpMatch.Overload([]types.BaseType{types.Text, types.TextS}, types.TextA, func(vectors []types.INullableVector, env map[string]string) (vector types.INullableVector, e error) {
		output := &types.NullableTextArray{}
		input := vectors[0].(*types.NullableText)
		reStr := vectors[0].Index(0).(string)
		re, err := regexp.Compile(reStr)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("regexpMatch函数未能成功编译正则表达式 %s, %s", reStr, err.Error()))
		}
		return BroadCast1(vectors[0], output, func(i int) error {
			ret := re.FindStringSubmatch(input.Index(i).(string))
			if len(ret) > 0 {
				if len(ret) == 1 { // 仅匹配自身，不存在分组
					output.Seti(i, ret)
				} else {
					output.Seti(i, ret[1:])
				}
			} else {
				output.SetNull(i, true)
			}
			return nil
		})
	})
}
