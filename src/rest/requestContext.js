import {AsyncLocalStorage} from 'node:async_hooks';

/**
 * Holds the current snapshot's username (and any other request-scoped state)
 * so that the http helpers in requests.js can inject auth headers without
 * threading the username through every call. Each top-level snapshot run
 * does:
 *
 *   await requestContext.run({username}, async () => { ... });
 *
 * Concurrent snapshot workers in the same process get isolated stores via
 * AsyncLocalStorage — no module-level mutable state.
 */
export const requestContext = new AsyncLocalStorage();

export function currentUsername() {
    const store = requestContext.getStore();
    if (!store?.username) {
        throw new Error('No active username — wrap http calls in requestContext.run({username}, ...)');
    }
    return store.username;
}
