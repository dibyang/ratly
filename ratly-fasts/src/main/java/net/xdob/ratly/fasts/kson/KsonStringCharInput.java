/*
 * Copyright 2014 Ruediger Moeller.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.xdob.ratly.fasts.kson;

import java.util.Stack;

/**
 * implementation of char input on top of a String
 */
public class KsonStringCharInput implements KsonCharInput {
    CharSequence s;
    int pos;
    int end;
    Stack<KsonDeserializer.ParseStep> stack;

    public KsonStringCharInput(CharSequence s) {
        this.s = s;
        pos = 0;
        end = s.length();
    }

    public KsonStringCharInput(String s, int pos, int len) {
        this.s = s;
        this.pos = pos;
        this.end = pos+len;
    }

    /**
     * @return char or -1 for eof
     */
    @Override
    public int readChar() {
        if ( pos >= end )
            return -1;
        return s.charAt(pos++);
    }

    @Override
    public int peekChar() {
        if ( pos >= end )
            return -1;
        return s.charAt(pos);
    }

    @Override
    public int position() {
        return pos;
    }

    @Override
    public int back(int num) {
        pos -= num; return pos;
    }

    @Override
    public boolean isEof() {
        return pos >= s.length();
    }

    @Override
    public String getString(int pos, int length) {
        return s.subSequence(Math.max(0,pos),Math.min(s.length(),pos+length)).toString();
    }
}
