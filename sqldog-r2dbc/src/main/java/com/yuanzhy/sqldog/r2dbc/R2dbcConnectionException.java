package com.yuanzhy.sqldog.r2dbc;

import io.r2dbc.spi.R2dbcException;

class R2dbcConnectionException extends R2dbcException {

    /**
     * Creates a new {@link R2dbcConnectionException}.
     */
    public R2dbcConnectionException() {
        super();
    }

    /**
     * Creates a new {@link R2dbcConnectionException}.
     *
     * @param reason the reason for the error.  Set as the exception's message and retrieved with {@link #getMessage()}.
     */
    public R2dbcConnectionException(String reason) {
        super(reason);
    }

    /**
     * Creates a new {@link R2dbcConnectionException}.
     *
     * @param reason   the reason for the error.  Set as the exception's message and retrieved with {@link #getMessage()}.
     * @param sqlState the "SQLState" string, which follows either the XOPEN SQLState conventions or the SQL:2003
     *                 conventions
     */
    public R2dbcConnectionException(String reason, String sqlState) {
        super(reason, sqlState);
    }

    /**
     * Creates a new {@link R2dbcConnectionException}.
     *
     * @param reason    the reason for the error.  Set as the exception's message and retrieved with {@link #getMessage()}.
     * @param sqlState  the "SQLState" string, which follows either the XOPEN SQLState conventions or the SQL:2003
     *                  conventions
     * @param errorCode a vendor-specific error code representing this failure
     */
    public R2dbcConnectionException(String reason, String sqlState, int errorCode) {
        super(reason, sqlState, errorCode);
    }

    /**
     * Creates a new {@link R2dbcConnectionException}.
     *
     * @param reason    the reason for the error.  Set as the exception's message and retrieved with {@link #getMessage()}.
     * @param sqlState  the "SQLState" string, which follows either the XOPEN SQLState conventions or the SQL:2003
     *                  conventions
     * @param errorCode a vendor-specific error code representing this failure
     * @param sql       the SQL statement that caused this error
     * @since 0.9
     */
    public R2dbcConnectionException(String reason, String sqlState, int errorCode, String sql) {
        super(reason, sqlState, errorCode, sql);
    }

    /**
     * Creates a new {@link R2dbcConnectionException}.
     *
     * @param reason    the reason for the error.  Set as the exception's message and retrieved with {@link #getMessage()}.
     * @param sqlState  the "SQLState" string, which follows either the XOPEN SQLState conventions or the SQL:2003
     *                  conventions
     * @param errorCode a vendor-specific error code representing this failure
     * @param sql       the SQL statement that caused this error
     * @param cause     the cause
     * @since 0.9
     */
    public R2dbcConnectionException(String reason, String sqlState, int errorCode, String sql, Throwable cause) {
        super(reason, sqlState, errorCode, sql, cause);
    }

    /**
     * Creates a new {@link R2dbcConnectionException}.
     *
     * @param reason    the reason for the error.  Set as the exception's message and retrieved with {@link #getMessage()}.
     * @param sqlState  the "SQLState" string, which follows either the XOPEN SQLState conventions or the SQL:2003
     *                  conventions
     * @param errorCode a vendor-specific error code representing this failure
     * @param cause     the cause
     */
    public R2dbcConnectionException(String reason, String sqlState, int errorCode, Throwable cause) {
        super(reason, sqlState, errorCode, cause);
    }

    /**
     * Creates a new {@link R2dbcConnectionException}.
     *
     * @param reason   the reason for the error.  Set as the exception's message and retrieved with {@link #getMessage()}.
     * @param sqlState the "SQLState" string, which follows either the XOPEN SQLState conventions or the SQL:2003
     *                 conventions
     * @param cause    the cause
     */
    public R2dbcConnectionException(String reason, String sqlState, Throwable cause) {
        super(reason, sqlState, cause);
    }

    /**
     * Creates a new {@link R2dbcConnectionException}.
     *
     * @param reason the reason for the error.  Set as the exception's message and retrieved with {@link #getMessage()}.
     * @param cause  the cause
     */
    public R2dbcConnectionException(String reason, Throwable cause) {
        super(reason, cause);
    }

    /**
     * Creates a new {@link R2dbcConnectionException}.
     *
     * @param cause the cause
     */
    public R2dbcConnectionException(Throwable cause) {
        super(cause);
    }
}
