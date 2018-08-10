/*******************************************************************************
 * * Copyright 2018 Impetus Infotech.
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 ******************************************************************************/
package com.impetus.fabric.jdbc;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Map;

import com.impetus.blkch.jdbc.BlkchnArray;

public class FabricArray implements BlkchnArray, Serializable{
    
    private static final long serialVersionUID = -7358910555095554719L;
    private Object[] array;
    
    public FabricArray(Object[] array) {
        this.array = array;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getBaseType() throws SQLException {
        if(array.length == 0) {
            return Types.JAVA_OBJECT;
        } else {
            Object element = array[0];
            if(element instanceof String) {
                return Types.VARCHAR;
            } else if(element instanceof Integer) {
                return Types.INTEGER;
            } else if(element instanceof Long) {
                return Types.BIGINT;
            } else if(element instanceof Double) {
                return Types.DOUBLE;
            } else if(element instanceof Float) {
                return Types.FLOAT;
            } else if(element instanceof Boolean) {
                return Types.BOOLEAN;
            } else {
                return Types.JAVA_OBJECT;
            }
        }
    }

    @Override
    public Object getArray() throws SQLException {
        return array;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        Object[] arr = new Object[count];
        long i = index;
        while(i < index + count) {
            if(i >= array.length) {
                break;
            }
            arr[(int)(i - index)] = array[(int)i];
        }
        return arr;
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void free() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
    
    @Override
    public String toString() {
        return Arrays.asList(array).toString();
    }

}
