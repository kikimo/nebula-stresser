package ltest

import (
	"fmt"
	"sort"

	"github.com/anishathalye/porcupine"
)

type kvInput struct {
	OP    uint8 // 0 => get, 1 => put, 2 => append
	Key   string
	Value string
}

type kvOutput struct {
	Value string
}

var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		fmt.Printf("partioning...\n")
		m := make(map[string][]porcupine.Operation)
		for _, v := range history {
			key := v.Input.(kvInput).Key
			m[key] = append(m[key], v)
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		ret := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			ret = append(ret, m[k])
		}
		return ret
	},
	PartitionEvent: func(history []porcupine.Event) [][]porcupine.Event {
		fmt.Printf("partioning...\n")
		m := make(map[string][]porcupine.Event)
		match := make(map[int]string) // id -> key
		for _, v := range history {
			if v.Kind == porcupine.CallEvent {
				key := v.Value.(kvInput).Key
				m[key] = append(m[key], v)
				match[v.Id] = key
			} else {
				key := match[v.Id]
				m[key] = append(m[key], v)
			}
		}
		var ret [][]porcupine.Event
		for _, v := range m {
			ret = append(ret, v)
		}
		return ret
	},
	Init: func() interface{} {
		// note: we are modeling a single key's value here;
		// we're partitioning by key, so this is okay
		return ""
	},
	Step: func(state, input, output interface{}) (bool, interface{}) {
		inp := input.(kvInput)
		out := output.(kvOutput)
		st := state.(string)
		if inp.OP == 0 {
			// get
			ok := out.Value == st
			// if !ok {
			// fmt.Printf("state size: %d, %s\n", len(st), st)
			// fmt.Printf("output size: %d, %s\n", len(out.Value), out.Value)
			// fmt.Printf("fuck, state: %+v, input: %+v, output: %+v\n", state, input, output)
			// }
			return ok, state
		} else if inp.OP == 1 {
			// put
			return true, inp.Value
		} else {
			// append
			return true, (st + inp.Value)
		}
	},
	DescribeOperation: func(input, output interface{}) string {
		inp := input.(kvInput)
		out := output.(kvOutput)
		switch inp.OP {
		case 0:
			return fmt.Sprintf("get('%s') -> '%s'", inp.Key, out.Value)
		case 1:
			return fmt.Sprintf("put('%s', '%s')", inp.Key, inp.Value)
		case 2:
			return fmt.Sprintf("append('%s', '%s')", inp.Key, inp.Value)
		default:
			return "<invalid>"
		}
	},
}
