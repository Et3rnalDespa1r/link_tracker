package parser

import (
	"errors"
	"strings"
)

var ErrUnsupportedLink = errors.New("unsupported link type")

type LinkType string

const (
	TypeGithub        LinkType = "github"
	TypeStackOverflow LinkType = "stackoverflow"
)

type ParsedLink struct {
	Type       LinkType
	Owner      string
	Repo       string
	QuestionID string
}

func ParseURL(rawURL string) (ParsedLink, error) {
	cleanURL := strings.TrimPrefix(rawURL, "https://")
	cleanURL = strings.TrimPrefix(cleanURL, "http://")

	parts := strings.Split(cleanURL, "/")

	if strings.HasPrefix(cleanURL, "github.com") {
		if len(parts) >= 3 {
			return ParsedLink{
				Type:  TypeGithub,
				Owner: parts[1],
				Repo:  parts[2],
			}, nil
		}
	} else if strings.HasPrefix(cleanURL, "stackoverflow.com") {
		if len(parts) >= 3 && parts[1] == "questions" {
			return ParsedLink{
				Type:       TypeStackOverflow,
				QuestionID: parts[2],
			}, nil
		}
	}

	return ParsedLink{}, ErrUnsupportedLink
}
