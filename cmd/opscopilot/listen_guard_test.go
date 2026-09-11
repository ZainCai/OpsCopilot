// D1 决策 C 门禁测试：非回环 + 无密钥必须拒绝；回环/有密钥/显式豁免放行。
package main

import "testing"

func TestCheckListenSecurity(t *testing.T) {
	cases := []struct {
		name    string
		addr    string
		token   string
		allow   bool
		wantErr bool
	}{
		{"loopback default ok", "127.0.0.1:8080", "", false, false},
		{"localhost ok", "localhost:8080", "", false, false},
		{"ipv6 loopback ok", "[::1]:8080", "", false, false},
		{"wildcard no token rejected", "0.0.0.0:8080", "", false, true},
		{"colon-form no token rejected (binds all interfaces)", ":8080", "", false, true},
		{"nic ip no token rejected", "10.0.0.5:8080", "", false, true},
		{"wildcard with token ok", "0.0.0.0:8080", "tk", false, false},
		{"wildcard no token explicit allow", "0.0.0.0:8080", "", true, false},
		{"blank addr rejected (defensive: binds all interfaces)", "", "", false, true},
	}
	for _, c := range cases {
		err := checkListenSecurity(c.addr, c.token, c.allow)
		if (err != nil) != c.wantErr {
			t.Fatalf("%s: err = %v, wantErr=%v", c.name, err, c.wantErr)
		}
	}
}

func TestIsLoopbackHost(t *testing.T) {
	for _, h := range []string{"127.0.0.1", "127.9.9.9", "localhost", "LOCALHOST", "::1", "[::1]"} {
		if !isLoopbackHost(h) {
			t.Fatalf("%q should be loopback", h)
		}
	}
	for _, h := range []string{"0.0.0.0", "10.0.0.5", "example.com", ""} {
		if isLoopbackHost(h) {
			t.Fatalf("%q should NOT be loopback", h)
		}
	}
}
