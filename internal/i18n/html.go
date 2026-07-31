package i18n

import (
	"context"
	"strings"
	"unicode"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

var translatableAttributes = map[string]struct{}{
	"aria-label": {},
	"placeholder": {},
	"title":       {},
}

// LocalizeHTML translates text and presentation attributes in server-rendered
// HTML. Script, style, code and user-editable text areas are deliberately left
// untouched. It is also safe for HTMX fragments.
func LocalizeHTML(ctx context.Context, source string) string {
	if source == "" || !HasLocale(ctx) {
		return source
	}
	if strings.Contains(strings.ToLower(source), "<!doctype html") {
		doc, err := html.Parse(strings.NewReader(source))
		if err != nil {
			return source
		}
		localizeNode(ctx, doc, false)
		var out strings.Builder
		if err := html.Render(&out, doc); err != nil {
			return source
		}
		return out.String()
	}

	contextNode := &html.Node{Type: html.ElementNode, Data: "div", DataAtom: atom.Div}
	nodes, err := html.ParseFragment(strings.NewReader(source), contextNode)
	if err != nil {
		return source
	}
	var out strings.Builder
	for _, node := range nodes {
		localizeNode(ctx, node, false)
		if err := html.Render(&out, node); err != nil {
			return source
		}
	}
	return out.String()
}

func localizeNode(ctx context.Context, node *html.Node, skip bool) {
	if node.Type == html.ElementNode {
		switch node.DataAtom {
		case atom.Script, atom.Style, atom.Code, atom.Pre, atom.Textarea:
			skip = true
		}
		if !skip {
			for index := range node.Attr {
				if _, ok := translatableAttributes[node.Attr[index].Key]; ok {
					node.Attr[index].Val = translateWhitespacePreserving(ctx, node.Attr[index].Val)
				}
			}
		}
	}
	if node.Type == html.TextNode && !skip {
		node.Data = translateWhitespacePreserving(ctx, node.Data)
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		localizeNode(ctx, child, skip)
	}
}

func translateWhitespacePreserving(ctx context.Context, value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return value
	}
	translated := T(ctx, trimmed)
	if translated == trimmed {
		return value
	}
	start := len(value) - len(strings.TrimLeftFunc(value, unicode.IsSpace))
	end := len(strings.TrimRightFunc(value, unicode.IsSpace))
	return value[:start] + translated + value[end:]
}
