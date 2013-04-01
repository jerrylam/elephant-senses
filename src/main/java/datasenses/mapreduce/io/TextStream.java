package datasenses.mapreduce.io;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import javax.activation.UnsupportedDataTypeException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;

public class TextStream extends Text {

	private final DataOutputStream out;

	private static ThreadLocal<CharsetDecoder> DECODER_FACTORY = new ThreadLocal<CharsetDecoder>() {
		protected CharsetDecoder initialValue() {
			return Charset.forName("UTF-8").newDecoder()
					.onMalformedInput(CodingErrorAction.REPORT)
					.onUnmappableCharacter(CodingErrorAction.REPORT);
		}
	};

	public TextStream(DataOutputStream out) {
		this.out = out;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// streamLength = WritableUtils.readVInt(in);
		in.readLine();
		CharsetDecoder decoder = DECODER_FACTORY.get();
		if (in instanceof DataInputStream)
			IOUtils.copy(new BufferedReader(new InputStreamReader((DataInputStream) in, decoder)), out);
		else {
			throw new UnsupportedDataTypeException();
		}
	}
}
