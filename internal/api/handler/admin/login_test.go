package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"

	mw "github.com/pablojhp.pergo/internal/api/middleware"
)

type stubAdminSessionStore struct {
	createdID     string
	createdExpiry time.Time
	createErr     error
	revokedID     string
	revokedAt     time.Time
	revokeErr     error
}

func (s *stubAdminSessionStore) CreateAdminSession(
	_ context.Context,
	sessionID string,
	expiresAt time.Time,
) error {
	s.createdID = sessionID
	s.createdExpiry = expiresAt
	return s.createErr
}

func (s *stubAdminSessionStore) RevokeAdminSession(
	_ context.Context,
	sessionID string,
	now time.Time,
) error {
	s.revokedID = sessionID
	s.revokedAt = now
	return s.revokeErr
}

func TestLoginPostRejectsOversizedBodyBeforeFormParsing(t *testing.T) {
	req := httptest.NewRequest(
		http.MethodPost,
		"/admin/login",
		strings.NewReader("password="+strings.Repeat("x", int(maxLoginBodyBytes))),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)

	if err := LoginPost(c, nil, "correct-password"); err != nil {
		t.Fatalf("LoginPost: %v", err)
	}
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d, want 413", rec.Code)
	}
}

func TestLoginPostAcceptsBoundedValidForm(t *testing.T) {
	form := url.Values{"password": {"correct-password"}}
	req := httptest.NewRequest(
		http.MethodPost,
		"/admin/login",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)

	if err := LoginPost(c, nil, "correct-password"); err != nil {
		t.Fatalf("LoginPost: %v", err)
	}
	if rec.Code != http.StatusFound {
		t.Fatalf("status=%d, want 302", rec.Code)
	}
	if len(rec.Result().Cookies()) != 1 {
		t.Fatal("valid login did not set a session cookie")
	}
}

func TestLoginPersistsSessionBeforeSendingCookieAndLogoutRevokesIt(t *testing.T) {
	store := &stubAdminSessionStore{}
	form := url.Values{"password": {"correct-password"}}
	req := httptest.NewRequest(
		http.MethodPost,
		"/admin/login",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)

	if err := LoginPost(c, store, "correct-password"); err != nil {
		t.Fatalf("LoginPost: %v", err)
	}
	if store.createdID == "" || store.createdExpiry.Before(time.Now().UTC()) {
		t.Fatalf("session was not durably created: %+v", store)
	}
	cookies := rec.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("cookies = %d, want 1", len(cookies))
	}
	identity, ok := mw.SessionIdentityFromCookie(
		cookies[0].Value,
		mw.GetSessionSecret(),
	)
	if !ok || identity.ID != store.createdID {
		t.Fatalf("cookie identity = %+v ok=%v, stored ID=%q", identity, ok, store.createdID)
	}

	logoutReq := httptest.NewRequest(http.MethodPost, "/admin/logout", nil)
	logoutReq.AddCookie(cookies[0])
	logoutRec := httptest.NewRecorder()
	logoutContext := echo.New().NewContext(logoutReq, logoutRec)
	if err := Logout(logoutContext, store); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	if store.revokedID != store.createdID || store.revokedAt.IsZero() {
		t.Fatalf("revocation = id:%q at:%v, want id:%q", store.revokedID, store.revokedAt, store.createdID)
	}
}

func TestLoginFailsClosedBeforeCookieWhenSessionStoreIsUnavailable(t *testing.T) {
	store := &stubAdminSessionStore{createErr: errors.New("database unavailable")}
	form := url.Values{"password": {"correct-password"}}
	req := httptest.NewRequest(
		http.MethodPost,
		"/admin/login",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)

	if err := LoginPost(c, store, "correct-password"); err != nil {
		t.Fatalf("LoginPost: %v", err)
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if len(rec.Result().Cookies()) != 0 {
		t.Fatal("login emitted a cookie before durable session creation")
	}
}

func TestLogoutFailsClosedWhenRevocationCannotBePersisted(t *testing.T) {
	store := &stubAdminSessionStore{revokeErr: errors.New("database unavailable")}
	loginReq := httptest.NewRequest(http.MethodPost, "/admin/login", nil)
	loginRec := httptest.NewRecorder()
	loginContext := echo.New().NewContext(loginReq, loginRec)
	mw.SetSessionCookie(loginContext, mw.GetSessionSecret())

	req := httptest.NewRequest(http.MethodPost, "/admin/logout", nil)
	req.AddCookie(loginRec.Result().Cookies()[0])
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)
	if err := Logout(c, store); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}
