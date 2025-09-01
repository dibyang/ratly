package net.xdob.ratly.util;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;


public class Printer4Proto {
	static final Logger LOG = LoggerFactory.getLogger(Printer4Proto.class);
	public static void printJson(MessageOrBuilder message, Consumer<String> consumer) {
		JsonFormat.Printer printer = JsonFormat.printer()
				.includingDefaultValueFields()
				.omittingInsignificantWhitespace();
		try {
			String json = printer.print(message);
			consumer.accept(json);
		} catch (InvalidProtocolBufferException e) {
			LOG.warn("print error", e);
		}
	}

	public static void printText(MessageOrBuilder message, Consumer<String> consumer) {
		TextFormat.Printer printer = TextFormat.printer();
		String text = printer.printToString(message);
		consumer.accept(text);
	}
}
