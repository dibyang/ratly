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
package net.xdob.ratly.fasts.offheap.structs;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 16.07.13
 * Time: 10:32
 * To change this template use File | Settings | File Templates.
 */
// FIXME: usage of this has bad test coverage ! COnsider remove. Alignament can be done inserting dummy fields
@Retention(RetentionPolicy.RUNTIME)
public @interface Align {
    int value();
}
