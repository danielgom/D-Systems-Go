package auth

import (
	"fmt"

	"github.com/casbin/casbin/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer interface {
	Authorize(subject, object, action string) error
}

type CasbinAuth struct {
	enforcer *casbin.Enforcer
}

// New uses model which will configure casbin mechanism
// policy is a file containing an ACL(Access Control List) which for this
// project will be a CSV file
func New(model, policy string) (*CasbinAuth, error) {
	enforcer, err := casbin.NewEnforcer(model, policy)
	if err != nil {
		return nil, err
	}

	return &CasbinAuth{enforcer: enforcer}, nil
}

func (a *CasbinAuth) Authorize(subject, object, action string) error {
	enforce, err := a.enforcer.Enforce(subject, object, action)
	if err != nil {
		return err
	}

	if !enforce {
		msg := fmt.Sprintf("%s not permitted to %s to %s", subject, action, object)
		st := status.New(codes.PermissionDenied, msg)
		return st.Err()
	}

	return nil
}
