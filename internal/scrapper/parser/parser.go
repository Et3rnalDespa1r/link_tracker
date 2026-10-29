package parser

import (
	"errors"
	"net/url"
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
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil {
		return ParsedLink{}, ErrUnsupportedLink
	}
	cleanURL := strings.ToLower(u.Hostname()) + u.Path

	parts := strings.Split(cleanURL, "/")

	if u.Hostname() == "github.com" {
		if len(parts) >= 3 {
			return ParsedLink{
				Type:  TypeGithub,
				Owner: parts[1],
				Repo:  parts[2],
			}, nil
		}
	} else if u.Hostname() == "stackoverflow.com" {
		if len(parts) >= 3 && parts[1] == "questions" {
			return ParsedLink{
				Type:       TypeStackOverflow,
				QuestionID: parts[2],
			}, nil
		}
	}

	return ParsedLink{}, ErrUnsupportedLink
}
