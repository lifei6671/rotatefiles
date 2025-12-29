package rotatefiles

import (
	"log"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestDefaultRotateRules(t *testing.T) {
	tests := []struct {
		name     string
		ruleName string
		base     string
		files    []string
		want     []string
	}{
		{
			name:     "test1",
			ruleName: "1hour",
			base:     "test.log",
			files:    []string{"test.log.2025122901", "test.log.2025122902", "test.log.2025122903", "test.log.2025122904", "test.log.121212"},
			want:     []string{"test.log.2025122901", "test.log.2025122902", "test.log.2025122903", "test.log.2025122904"},
		},
		{
			name:     "test2",
			ruleName: "30min",
			base:     "test.log",
			files:    []string{"test.log.202512290100", "test.log.202512290201", "test.log.202512290302", "test.log.202512293003", "test.log.12112"},
			want:     []string{"test.log.202512290100", "test.log.202512290201", "test.log.202512290302", "test.log.202512293003"},
		},
	}
	for _, tt := range tests {
		rule, ok := defaultRotateRules[tt.ruleName]

		if !ok {
			t.Errorf("rule %s not found", tt.ruleName)
		}
		t.Run(tt.name, func(t *testing.T) {
			var matched []string
			for _, name := range tt.files {
				if filepath.Base(name) == tt.base {
					continue // 原始软链文件不删
				}
				if !strings.HasPrefix(name, tt.base) {
					// 不存在指定的前缀不处理
					continue
				}
				suffix := name[len(tt.base):]
				log.Println(suffix)
				// 如果正则能匹配到则说明要清理
				if rule.SuffixExpr.MatchString(suffix) {
					matched = append(matched, name)
				}
			}
			if !reflect.DeepEqual(matched, tt.want) {
				t.Errorf("want %v, got %v", tt.want, matched)
			}
		})
	}
}
