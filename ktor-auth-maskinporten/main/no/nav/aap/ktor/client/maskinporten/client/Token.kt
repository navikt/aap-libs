package no.nav.aap.ktor.client.maskinporten.client

import com.nimbusds.jwt.SignedJWT
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.Logger
import java.time.Instant

internal data class Token(val access_token: String) {
    private val signedJwt = SignedJWT.parse(access_token)
    private val expiry = signedJwt.jwtClaimsSet.expirationTime.toInstant().minusSeconds(LEEWAY_SECONDS)

    fun hasExpired() = expiry < Instant.now()
}

internal class TokenCache {
    private val tokens: HashMap<String, Token> = hashMapOf()
    private val mutex = Mutex()

    internal fun logg(logger: Logger) {
        tokens.forEach { (key, value) ->
            logger.info("Key: $key, Value: $value")
        }
    }

    internal suspend fun add(key: String, token: Token) {
        mutex.withLock {
            tokens[key] = token
        }
    }

    internal suspend fun get(key: String): Token? {
        mutex.withLock {
            tokens[key]
        }?.let {
            if (it.hasExpired()) {
                rm(key)
            }
        }

        return mutex.withLock {
            tokens[key]
        }
    }

    private suspend fun rm(key: String) {
        mutex.withLock {
            tokens.remove(key)
        }
    }
}