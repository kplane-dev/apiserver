package auth

import (
	"sync"
	"time"
)

const tokenReviewHintTTL = 15 * time.Second

type tokenHintEntry struct {
	clusterID string
	expiresAt time.Time
}

var tokenReviewHints sync.Map // map[token]tokenHintEntry

// RememberTokenReviewHint stores a short-lived token->cluster mapping.
// Multicluster admission can call this before TokenReview storage invokes the
// authenticator on a synthetic request that does not carry cluster context.
func RememberTokenReviewHint(token, clusterID string) {
	rememberTokenReviewHint(token, clusterID)
}

func rememberTokenReviewHint(token, clusterID string) {
	if token == "" || clusterID == "" {
		return
	}
	tokenReviewHints.Store(token, tokenHintEntry{
		clusterID: clusterID,
		expiresAt: time.Now().Add(tokenReviewHintTTL),
	})
}

func clusterHintForToken(token string) string {
	if token == "" {
		return ""
	}
	v, ok := tokenReviewHints.Load(token)
	if !ok {
		return ""
	}
	entry, ok := v.(tokenHintEntry)
	if !ok {
		tokenReviewHints.Delete(token)
		return ""
	}
	if time.Now().After(entry.expiresAt) {
		tokenReviewHints.Delete(token)
		return ""
	}
	return entry.clusterID
}
