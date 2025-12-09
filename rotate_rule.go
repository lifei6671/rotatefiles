package rotatefiles

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

type rotateRule struct {
	SuffixFunc func() string
	SuffixExpr *regexp.Regexp
	Duration   time.Duration
}

var defaultRotateRules = map[string]*rotateRule{
	"1hour": {
		Duration: 1 * time.Hour,
		// 小时  后缀如  .2025040117
		SuffixFunc: func() string {
			return "." + time.Now().Format("2006010215")
		},
		SuffixExpr: regexp.MustCompile(`^\.[]0-9]{10}$`),
	},
	"1day": {
		Duration: 24 * time.Hour,
		// 天  后缀如  .20250401
		SuffixFunc: func() string {
			return "." + time.Now().Format("20060102")
		},
		SuffixExpr: regexp.MustCompile(`^\.[0-9]{8}$`),
	},
	"no": {
		Duration: 0,
		// 无后缀
		SuffixFunc: func() string {
			return ""
		},
		SuffixExpr: nil,
	},
	"1min": {
		Duration: 1 * time.Minute,
		// 1分钟 后缀如  .202504011700  .202504011701  .202504011702  .202504011759
		SuffixFunc: func() string {
			now := time.Now()
			return "." + now.Format("2006010215") + fmt.Sprintf("%02d", now.Minute())
		},
		// "." + YYYYMMDDHHMM (12 位数字)
		SuffixExpr: regexp.MustCompile(`^\.[0-9]{12}$`),
	},
	"5min": {
		Duration: 5 * time.Minute,
		// 5分钟 后缀如  .202504011700  .202504011705  .202504011710  .202504011715
		SuffixFunc: func() string {
			now := time.Now()
			return "." + now.Format("2006010215") + fmt.Sprintf("%02d", now.Minute()/5*5)
		},
		SuffixExpr: regexp.MustCompile(`^\.[0-9]{12}$`),
	},
	"10min": {
		Duration: 10 * time.Minute,
		// 10分钟 后缀如  .202504011700  .202504011710  .202504011720  .202504011750
		SuffixFunc: func() string {
			now := time.Now()
			return "." + now.Format("2006010215") + fmt.Sprintf("%02d", now.Minute()/10*10)
		},
		SuffixExpr: regexp.MustCompile(`^\.[0-9]{10}$`),
	},
	"15min": {
		Duration: 15 * time.Minute,
		// 15分钟 后缀如  .202504011700  .202504011715  .202504011730  .202504011745
		SuffixFunc: func() string {
			now := time.Now()
			return "." + now.Format("2006010215") + fmt.Sprintf("%02d", now.Minute()/15*15)
		},
		SuffixExpr: regexp.MustCompile(`^\.[0-9]{15}$`),
	},
	"30min": {
		Duration: 30 * time.Minute,
		// 30分钟 后缀如  .202504011700  .202504011730
		SuffixFunc: func() string {
			now := time.Now()
			return "." + now.Format("2006010215") + fmt.Sprintf("%02d", now.Minute()/30*30)
		},
		SuffixExpr: regexp.MustCompile(`^\.[0-9]{30}$`),
	},
}

// RegisterRotateRuleWithExpr 注册新的文件切分规则
//
// 已内置的规则：
//
//	1hour -> 1小时  后缀如  .2025040117
//	1day  -> 1天    后缀如  .20250401
//	no    -> 无后缀
//	1min  -> 1分钟  后缀如  .202504011700  .202504011701  .202504011702  .202504011759
//	5min  -> 5分钟  后缀如  .202504011700  .202504011705  .202504011710  .202504011715
//	10min -> 10分钟 后缀如  .202504011700  .202504011710  .202504011720  .202504011750
//	30min -> 30分钟 后缀如  .202504011700  .202504011730
//
//	若当前时间是 2020年07月22日 17点34
//	如选择规则 "1hour", 内容会输出到 xxx.2025040117
//	如选择规则 "5min",  内容会输出到 xxx.202504011730
//	如选择规则 "30min", 内容会输出到 xxx.202504011730
func RegisterRotateRuleWithExpr(
	rule string,
	duration time.Duration,
	suffix func() string,
	suffixExpr *regexp.Regexp,
) error {
	if _, has := defaultRotateRules[rule]; has {
		return errors.New("rule already exists")
	}
	defaultRotateRules[rule] = &rotateRule{
		Duration:   duration,
		SuffixFunc: suffix,
		SuffixExpr: suffixExpr,
	}
	return nil
}
