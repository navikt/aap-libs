package no.nav.aap.ktor.client

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

internal data class Token(val expires_in: Long, val access_token: String) {
    private val expiry: Instant = Instant.now().plusSeconds(expires_in - LEEWAY_SECONDS)

    internal fun expired() = Instant.now().isAfter(expiry)

    private companion object {
        const val LEEWAY_SECONDS = 60
    }
}

internal class TokenCache<K> {
    private val tokens: HashMap<K, Token> = hashMapOf()
    private val mutex = Mutex()

    internal suspend fun add(key: K, token: Token) {
        mutex.withLock {
            tokens[key] = token
        }
    }

    internal suspend fun get(key: K): Token? {
        mutex.withLock {
            tokens[key]
        }?.let {
            if (it.expired()) {
                rm(key)
            }
        }

        return mutex.withLock {
            tokens[key]
        }
    }

    private suspend fun rm(key: K) {
        mutex.withLock {
            tokens.remove(key)
        }
    }
}
