package readonly

import (
	"errors"
	"testing"
	"time"
)

func TestValidate(t *testing.T) {
	cases := []struct {
		name string
		cred Credential
		want error
	}{
		{
			name: "合法只读凭证",
			cred: Credential{ID: "c1", Secret: "s", ReadOnly: true},
			want: nil,
		},
		{
			name: "写权限凭证必须被拒",
			cred: Credential{ID: "c1", Secret: "s", ReadOnly: false},
			want: ErrNotReadOnly,
		},
		{
			name: "空 ID 必须被拒",
			cred: Credential{ID: "", Secret: "s", ReadOnly: true},
			want: ErrEmptyID,
		},
		{
			name: "已过期凭证必须被拒",
			cred: Credential{ID: "c1", Secret: "s", ReadOnly: true, ExpiresAt: time.Now().Add(-time.Minute)},
			want: ErrExpired,
		},
		{
			// 边界：非只读优先于过期——先守住只读这条底线
			name: "既非只读又已过期，优先报非只读",
			cred: Credential{ID: "c1", ReadOnly: false, ExpiresAt: time.Now().Add(-time.Minute)},
			want: ErrNotReadOnly,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.cred)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("want nil, got %v", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
}

func TestExpired(t *testing.T) {
	// 零值 ExpiresAt 视为永不过期
	if (Credential{}).Expired(time.Now()) {
		t.Error("zero ExpiresAt must not be treated as expired")
	}
	future := Credential{ExpiresAt: time.Now().Add(time.Hour)}
	if future.Expired(time.Now()) {
		t.Error("future ExpiresAt should not be expired")
	}
	past := Credential{ExpiresAt: time.Now().Add(-time.Hour)}
	if !past.Expired(time.Now()) {
		t.Error("past ExpiresAt should be expired")
	}
}

func TestNewBearer(t *testing.T) {
	c := NewBearer("id-1", "tok")
	if !c.ReadOnly {
		t.Error("NewBearer must produce read-only credential")
	}
	if c.ID != "id-1" || c.Secret != "tok" {
		t.Errorf("ID/Secret mismatch: %q/%q", c.ID, c.Secret)
	}
	if err := Validate(c); err != nil {
		t.Errorf("NewBearer output must pass Validate, got %v", err)
	}
}
