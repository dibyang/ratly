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

package net.xdob.ratly.fasts.serialization.serializers;

import net.xdob.ratly.fasts.serialization.FSTClazzInfo;
import net.xdob.ratly.fasts.serialization.FSTObjectInput;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 12.11.12
 * Time: 01:20
 * To change this template use File | Settings | File Templates.
 */
public class FSTStringBuilderSerializer extends FSTStringBufferSerializer {
    @Override
    public Object instantiate(Class objectClass, FSTObjectInput in, FSTClazzInfo serializationInfo, FSTClazzInfo.FSTFieldInfo referencee, int streamPosition) throws Exception {
        String s = in.readStringUTF();
        StringBuilder stringBuilder = new StringBuilder(s);
        in.registerObject(stringBuilder, streamPosition,serializationInfo, referencee);
        return stringBuilder;
    }
}
