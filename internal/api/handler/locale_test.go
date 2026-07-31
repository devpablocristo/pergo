package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
)

func TestLocalePostStoresValidatedCookieAndKeepsLocalReferer(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/locale", nil)
	req.Header.Set("Referer", "http://example.test/admin/devices?tab=all")
	req.Host = "example.test"
	req.Form = map[string][]string{"locale": {"en"}}
	rec := httptest.NewRecorder()
	if err := LocalePost(e.NewContext(req, rec)); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/admin/devices?tab=all" {
		t.Fatalf("redirect = %d %q", rec.Code, rec.Header().Get("Location"))
	}
	cookies := rec.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Value != "en" || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteLaxMode {
		t.Fatalf("unexpected locale cookie: %#v", cookies)
	}
}

func TestLocalePostRejectsInvalidLocaleAndExternalReferer(t *testing.T) {
	e := echo.New()
	invalid := httptest.NewRequest(http.MethodPost, "/locale", nil)
	invalid.Form = map[string][]string{"locale": {"fr"}}
	invalidRec := httptest.NewRecorder()
	if err := LocalePost(e.NewContext(invalid, invalidRec)); err != nil {
		t.Fatal(err)
	}
	if invalidRec.Code != http.StatusBadRequest || len(invalidRec.Result().Cookies()) != 0 {
		t.Fatalf("invalid locale response = %d, cookies=%v", invalidRec.Code, invalidRec.Result().Cookies())
	}

	valid := httptest.NewRequest(http.MethodPost, "/locale", nil)
	valid.Header.Set("Referer", "https://attacker.example/steal")
	valid.Host = "example.test"
	valid.Form = map[string][]string{"locale": {"es"}}
	validRec := httptest.NewRecorder()
	if err := LocalePost(e.NewContext(valid, validRec)); err != nil {
		t.Fatal(err)
	}
	if validRec.Header().Get("Location") != "/" {
		t.Fatalf("external referer location = %q", validRec.Header().Get("Location"))
	}
}
