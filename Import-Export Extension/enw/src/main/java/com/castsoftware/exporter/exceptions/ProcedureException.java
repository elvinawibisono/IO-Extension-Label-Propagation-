/*
 *  Friendly exporter for Neo4j - Copyright (C) 2020  Hugo JOBY
 *
 *      This library is free software; you can redistribute it and/or modify it under the terms
 *      of the GNU Lesser General Public License as published by the Free Software Foundation;
 *      either version 2.1 of the License, or (at your option) any later version.
 *      This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *      without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *      See the GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License along with this library;
 *      If not, see <https://www.gnu.org/licenses/>.
 */

package com.castsoftware.exporter.exceptions;


import org.neo4j.logging.Log;

/**
 * The <code>ProcedureException</code> is used for exception while executing a procedure
 * when a exception case happened, it automatically logs the exception with the associated stack trace.
 * A message and an error code are thrown to the Neo4j console.
 * ProcedureException
 */
public class ProcedureException extends Throwable {

    private static final long serialVersionUID = 2486261902406071343L;
    private static final String DEFAULT_USER_ERROR_MESSAGE = "An error occurred during the execution of the procedure. See logs for more details. Code (%s)";
    private static final String DEFAULT_LOG_ERROR_MESSAGE = "Fatal error, the procedure stopped (Code %s) . %s";

    private final String code;
    private final String message;
    private final Throwable cause;

    public void logException(Log log) {
        log.error(this.message, this.cause);
    }
    /**
     * ProcedureException constructor
     * @param message Exception message that will be log.
     * @param cause Throwable object
     * @param code Code of the exception (Displayed in the user interface)
     */
    public ProcedureException(String message, Throwable cause, String code) {
        super(String.format(DEFAULT_USER_ERROR_MESSAGE, code));
        this.message = String.format(DEFAULT_LOG_ERROR_MESSAGE, code, message);
        this.cause = cause;
        this.code = code;
    }

    public ProcedureException(String message, Throwable cause) {
        super(String.format(DEFAULT_USER_ERROR_MESSAGE, "0000x0000"));
        if (cause instanceof ExporterException) {
            this.code = ((ExporterException) cause).code;
        } else {
            this.code = null;
        }
        this.message = String.format(DEFAULT_LOG_ERROR_MESSAGE, this.code, message);
        this.cause = cause;

    }

    public ProcedureException(ExporterException e) {
        super(String.format(DEFAULT_USER_ERROR_MESSAGE, e.getCode()));
        this.message = e.getMessage();
        this.cause = e.getCause();
        this.code = e.getCode();
    }

    public ProcedureException(Throwable cause) {
        super(String.format(DEFAULT_USER_ERROR_MESSAGE, "-1"));
        this.message = "UNHANDLED EXCEPTION : "+cause.getMessage();
        this.cause = cause;
        this.code = "-1";
    }

    @Override
    public String toString() {
        return "ProcedureException{" +
                "code='" + code + '\'' +
                ", message='" + message + '\'';
    }

    public String getCode() {
        return this.code;
    }
}