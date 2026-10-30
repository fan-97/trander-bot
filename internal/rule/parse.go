package rule

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"gopkg.in/yaml.v3"

	"trader-bot/internal/indicator"
)

// Parse converts a YAML condition (mapping with all/any/not, or a leaf string)
// into a Node. Errors include the YAML line and the offending expression.
func Parse(n *yaml.Node) (Node, error) {
	if n == nil {
		return nil, errors.New("empty condition")
	}
	switch n.Kind {
	case yaml.DocumentNode:
		if len(n.Content) != 1 {
			return nil, errors.New("empty condition")
		}
		return Parse(n.Content[0])
	case yaml.AliasNode:
		return Parse(n.Alias)
	case yaml.ScalarNode:
		if n.Tag == "!!null" {
			return nil, fmt.Errorf("line %d: empty condition", n.Line)
		}
		l, err := parseLeaf(n.Value, n.Line)
		if err != nil {
			return nil, err
		}
		return l, nil
	case yaml.MappingNode:
		if len(n.Content) != 2 {
			return nil, fmt.Errorf("line %d: condition mapping must have exactly one key (all, any or not), got %d", n.Line, len(n.Content)/2)
		}
		key, val := n.Content[0], n.Content[1]
		switch key.Value {
		case "all", "any":
			if val.Kind == yaml.AliasNode {
				val = val.Alias
			}
			if val.Kind != yaml.SequenceNode {
				return nil, fmt.Errorf("line %d: %q expects a sequence of conditions", val.Line, key.Value)
			}
			if len(val.Content) == 0 {
				return nil, fmt.Errorf("line %d: %q needs at least one condition", key.Line, key.Value)
			}
			var kids []Node
			var errs []error
			for _, c := range val.Content {
				k, err := Parse(c)
				if err != nil {
					errs = append(errs, err)
					continue
				}
				kids = append(kids, k)
			}
			if len(errs) > 0 {
				return nil, errors.Join(errs...)
			}
			if key.Value == "all" {
				return All{Children: kids}, nil
			}
			return Any{Children: kids}, nil
		case "not":
			c := val
			if c.Kind == yaml.AliasNode {
				c = c.Alias
			}
			if c.Kind != yaml.MappingNode && c.Kind != yaml.ScalarNode {
				return nil, fmt.Errorf("line %d: \"not\" takes a single condition (mapping or string)", val.Line)
			}
			child, err := Parse(c)
			if err != nil {
				return nil, err
			}
			return Not{Child: child}, nil
		default:
			return nil, fmt.Errorf("line %d: unknown key %q (want all, any or not)", key.Line, key.Value)
		}
	default:
		return nil, fmt.Errorf("line %d: condition must be a mapping or string", n.Line)
	}
}

// ParseLeaf parses "<operand> <cmp> <operand>" or "cross_up(<a>, <b>)".
func ParseLeaf(s string) (Leaf, error) { return parseLeaf(s, 0) }

func parseLeaf(s string, line int) (Leaf, error) {
	p := &parser{src: s}
	l, err := p.leaf()
	if err != nil {
		prefix := ""
		if line > 0 {
			prefix = fmt.Sprintf("line %d: ", line)
		}
		return Leaf{}, fmt.Errorf("%sinvalid expression %q: %v", prefix, s, err)
	}
	return l, nil
}

type tokKind int

const (
	tEOF tokKind = iota
	tIdent
	tNum
	tLParen
	tRParen
	tComma
	tCmp
)

type token struct {
	kind tokKind
	text string
	num  float64
	pos  int // 0-based byte offset
}

type parser struct {
	src string
	i   int
	tok token
}

func isDigit(c byte) bool      { return c >= '0' && c <= '9' }
func isIdentStart(c byte) bool { return c == '_' || (c|0x20 >= 'a' && c|0x20 <= 'z') }

func (p *parser) errf(pos int, format string, a ...any) error {
	return fmt.Errorf("%s at position %d", fmt.Sprintf(format, a...), pos+1)
}

// next scans the next token into p.tok.
func (p *parser) next() error {
	s := p.src
	for p.i < len(s) && (s[p.i] == ' ' || s[p.i] == '\t') {
		p.i++
	}
	start := p.i
	if p.i >= len(s) {
		p.tok = token{kind: tEOF, pos: start}
		return nil
	}
	c := s[p.i]
	switch {
	case c == '(':
		p.i++
		p.tok = token{kind: tLParen, text: "(", pos: start}
	case c == ')':
		p.i++
		p.tok = token{kind: tRParen, text: ")", pos: start}
	case c == ',':
		p.i++
		p.tok = token{kind: tComma, text: ",", pos: start}
	case c == '>' || c == '<':
		p.i++
		if p.i < len(s) && s[p.i] == '=' {
			p.i++
		}
		p.tok = token{kind: tCmp, text: s[start:p.i], pos: start}
	case c == '=':
		if p.i+1 < len(s) && s[p.i+1] == '=' {
			p.i += 2
			p.tok = token{kind: tCmp, text: "==", pos: start}
		} else {
			return p.errf(start, "unexpected '=' (use ==)")
		}
	case isIdentStart(c):
		for p.i < len(s) && (isIdentStart(s[p.i]) || isDigit(s[p.i])) {
			p.i++
		}
		p.tok = token{kind: tIdent, text: s[start:p.i], pos: start}
	case isDigit(c) || c == '.' || c == '-':
		if c == '-' {
			p.i++
		}
		digits := 0
		for p.i < len(s) && isDigit(s[p.i]) {
			p.i++
			digits++
		}
		if p.i < len(s) && s[p.i] == '.' {
			p.i++
			for p.i < len(s) && isDigit(s[p.i]) {
				p.i++
				digits++
			}
		}
		if digits == 0 {
			return p.errf(start, "invalid number")
		}
		if p.i < len(s) && (s[p.i] == 'e' || s[p.i] == 'E') {
			j := p.i + 1
			if j < len(s) && (s[j] == '+' || s[j] == '-') {
				j++
			}
			if j < len(s) && isDigit(s[j]) {
				for j < len(s) && isDigit(s[j]) {
					j++
				}
				p.i = j
			}
		}
		v, err := strconv.ParseFloat(s[start:p.i], 64)
		if err != nil || math.IsInf(v, 0) || math.IsNaN(v) {
			return p.errf(start, "invalid number %q", s[start:p.i])
		}
		p.tok = token{kind: tNum, text: s[start:p.i], num: v, pos: start}
	default:
		return p.errf(start, "unexpected character %q", c)
	}
	return nil
}

func (p *parser) describe() string {
	if p.tok.kind == tEOF {
		return "end of expression"
	}
	return strconv.Quote(p.tok.text)
}

func (p *parser) leaf() (Leaf, error) {
	if err := p.next(); err != nil {
		return Leaf{}, err
	}
	if p.tok.kind == tIdent && (p.tok.text == "cross_up" || p.tok.text == "cross_down") {
		return p.cross()
	}
	left, err := p.operand()
	if err != nil {
		return Leaf{}, err
	}
	if p.tok.kind != tCmp {
		return Leaf{}, p.errf(p.tok.pos, "expected comparison operator (> >= < <= ==), got %s", p.describe())
	}
	op := Op(p.tok.text)
	if err := p.next(); err != nil {
		return Leaf{}, err
	}
	right, err := p.operand()
	if err != nil {
		return Leaf{}, err
	}
	if p.tok.kind != tEOF {
		return Leaf{}, p.errf(p.tok.pos, "unexpected %s after expression", p.describe())
	}
	return finish(Leaf{Left: left, Op: op, Right: right}, 0)
}

func (p *parser) expect(k tokKind, what string) error {
	if p.tok.kind != k {
		return p.errf(p.tok.pos, "expected %s, got %s", what, p.describe())
	}
	return p.next()
}

func (p *parser) cross() (Leaf, error) {
	op := Op(p.tok.text)
	if err := p.next(); err != nil {
		return Leaf{}, err
	}
	if err := p.expect(tLParen, `"("`); err != nil {
		return Leaf{}, err
	}
	left, err := p.operand()
	if err != nil {
		return Leaf{}, err
	}
	if err := p.expect(tComma, `","`); err != nil {
		return Leaf{}, err
	}
	right, err := p.operand()
	if err != nil {
		return Leaf{}, err
	}
	if p.tok.kind != tRParen {
		return Leaf{}, p.errf(p.tok.pos, `expected ")", got %s`, p.describe())
	}
	if err := p.next(); err != nil {
		return Leaf{}, err
	}
	if p.tok.kind != tEOF {
		return Leaf{}, p.errf(p.tok.pos, "unexpected %s after expression", p.describe())
	}
	return finish(Leaf{Left: left, Op: op, Right: right}, 0)
}

// finish validates leaf-level rules: at least one side must be a function.
func finish(l Leaf, _ int) (Leaf, error) {
	if l.Left.Func == "" && l.Right.Func == "" {
		return Leaf{}, errors.New("at least one side must be an indicator or data function, not two constants")
	}
	return l, nil
}

// operand parses name | name(num,...) | number, leaving p.tok after it.
func (p *parser) operand() (Operand, error) {
	switch p.tok.kind {
	case tNum:
		v := p.tok.num
		if err := p.next(); err != nil {
			return Operand{}, err
		}
		return Operand{Const: &v}, nil
	case tIdent:
		name, pos := p.tok.text, p.tok.pos
		if err := p.next(); err != nil {
			return Operand{}, err
		}
		var args []float64
		if p.tok.kind == tLParen {
			if err := p.next(); err != nil {
				return Operand{}, err
			}
			for p.tok.kind != tRParen {
				if len(args) > 0 {
					if err := p.expect(tComma, `"," or ")"`); err != nil {
						return Operand{}, err
					}
				}
				if p.tok.kind != tNum {
					return Operand{}, p.errf(p.tok.pos, "expected numeric argument, got %s", p.describe())
				}
				args = append(args, p.tok.num)
				if err := p.next(); err != nil {
					return Operand{}, err
				}
			}
			if err := p.next(); err != nil {
				return Operand{}, err
			}
		}
		if err := validateFunc(name, args); err != nil {
			return Operand{}, p.errf(pos, "%v", err)
		}
		return Operand{Func: name, Args: args}, nil
	default:
		return Operand{}, p.errf(p.tok.pos, "expected indicator or number, got %s", p.describe())
	}
}

func validateFunc(name string, args []float64) error {
	want, ok := contractArity[name]
	if !ok {
		_, err := indicator.New(name, args)
		return err
	}
	if len(args) != want {
		return fmt.Errorf("%s takes %d argument(s), got %d", name, want, len(args))
	}
	for _, a := range args {
		if a < 1 || a != math.Trunc(a) {
			return fmt.Errorf("%s argument must be a positive integer, got %s", name, fmtNum(a))
		}
	}
	return nil
}
