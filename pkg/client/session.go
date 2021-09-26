package client

import (
	"fmt"

	nebula "github.com/vesoft-inc/nebula-go/v2"
)

type SessionX struct {
	id int
	*nebula.Session
}

func (s *SessionX) checkResult(res *nebula.ResultSet) error {
	if !res.IsSucceed() {
		return fmt.Errorf("ErrorCode: %v, ErrorMsg: %s", res.GetErrorCode(), res.GetErrorMsg())
	}

	return nil
}

func (s *SessionX) Execute(stmt string) (*nebula.ResultSet, error) {
	ret, err := s.Session.Execute(stmt)
	if err != nil {
		return nil, err
	}

	if err := s.checkResult(ret); err != nil {
		return nil, err
	}

	return ret, nil
}

func (s *SessionX) GetID() int {
	return s.id
}

func New(id int, session *nebula.Session) *SessionX {
	return &SessionX{id: id, Session: session}
}
