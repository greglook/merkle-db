(ns merkle-db.lock
  "Locks offer a mechanism to ensure that only one process is working to update
  a database at a time. Lock managers can be backed by a system which offers a
  way to do atomic compare-and-set requests."
  (:require
    [clojure.spec :as s]))


;; ## Specs

(s/def ::client-id string?)
(s/def ::secret-key (s/and string? not-empty))
(s/def ::expires-at :time/instant)

(s/def ::info
  (s/keys :req [::client-id
                ::expires-at]
          :opt [::secret-key]))



;; ## Lock Protocol

(defprotocol ILockManager
  "A lock manager handles aquiring, refreshing, and releasing locks on database
  resources."

  (lock-info
    [locker lock-name]
    "Returns information about the currently-held lock on the database. Returns
    nil if the database is not currently locked.")

  (lock!
    [locker lock-name client-id duration]
    "Attempt to acquire a lock to update the database. The provided client-id
    is treated opaquely but should be a useful identifier. The duration is a
    requested period in seconds which the lock will expire after.

    Returns a lock info map on success, or throws an exception on failure with
    details about the current lock holder.")

  (renew-lock!
    [locker lock-name secret-key duration]
    "Renew a currently-held lock on the database by providing the key and a new
    requested duration. Returns a lock info map on success. Throws an exception
    if the lock is not held by this process.")

  (unlock!
    [locker lock-name secret-key]
    "Release the lock held on the named database. Throws an exception if the
    lock is not held by this process.")

  (force-unlock!
    [locker lock-name]
    "Forcibly open the named lock. Useful for manual interventions."))
