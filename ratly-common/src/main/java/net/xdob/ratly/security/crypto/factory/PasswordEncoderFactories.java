/*
 * Copyright 2002-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.xdob.ratly.security.crypto.factory;


import net.xdob.ratly.security.crypto.bcrypt.BCryptPasswordEncoder;
import net.xdob.ratly.security.crypto.password.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Used for creating {@link PasswordEncoder} instances
 *
 * @author Rob Winch
 * @since 5.0
 */
public final class PasswordEncoderFactories {

	private static final Logger logger = LoggerFactory.getLogger(PasswordEncoderFactories.class);

	private PasswordEncoderFactories() {
	}

	public static PasswordEncoder createDelegatingPasswordEncoder() {
		String encodingId = "bcrypt";
		Map<String, PasswordEncoder> encoders = new HashMap<>();
		encoders.put(encodingId, new BCryptPasswordEncoder());
		encoders.put("noop", NoOpPasswordEncoder.getInstance());
		encoders.put("pbkdf2", Pbkdf2PasswordEncoder.defaultsForSpringSecurity_v5_5());
		putIfAlgorithmSupported("pbkdf2@SpringSecurity_v5_8", Pbkdf2PasswordEncoder::defaultsForSpringSecurity_v5_8,
				encoders);
		encoders.put("SHA-1", new MessageDigestPasswordEncoder("SHA-1"));
		encoders.put("SHA-256",
				new MessageDigestPasswordEncoder("SHA-256"));
		encoders.put("sha256", new StandardPasswordEncoder());
		return new DelegatingPasswordEncoder(encodingId, encoders);
	}

	private static void putIfAlgorithmSupported(String encodingId, Supplier<PasswordEncoder> encoderSupplier,
			Map<String, PasswordEncoder> encoders) {
		try {
			PasswordEncoder passwordEncoder = encoderSupplier.get();
			encoders.put(encodingId, passwordEncoder);
		}
		catch (Exception ex) {
			if (ex.getCause() instanceof NoSuchAlgorithmException) {
				logger.warn(String.format(
						"Cannot create PasswordEncoder with encodingId [%s] because the algorithm is not available",
						encodingId), ex);
			}
			else {
				throw ex;
			}
		}
	}

}
