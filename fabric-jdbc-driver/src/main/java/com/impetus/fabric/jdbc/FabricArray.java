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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.Map;

import com.impetus.blkch.jdbc.BlkchnArray;

public class FabricArray implements BlkchnArray {
    
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
        throw new SQLFeatureNotSupportedException();
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
