package datasenses.mapreduce.io;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import javax.activation.UnsupportedDataTypeException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;

/**
 * Stream support for {@link org.apache.hadoop.io.Text}
 * @author JLam
 *
 */
public class TextStream extends Text {

	private final OutputStream out;

	private static ThreadLocal<CharsetDecoder> DECODER_FACTORY = new ThreadLocal<CharsetDecoder>() {
		protected CharsetDecoder initialValue() {
			return Charset.forName("UTF-8").newDecoder()
					.onMalformedInput(CodingErrorAction.REPORT)
					.onUnmappableCharacter(CodingErrorAction.REPORT);
		}
	};

	public TextStream(OutputStream out) {
		this.out = out;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		in.readLine();
		CharsetDecoder decoder = DECODER_FACTORY.get();
		if (in instanceof InputStream)
			IOUtils.copy(new BufferedReader(new InputStreamReader((InputStream) in, decoder)), out);
		else {
			throw new UnsupportedDataTypeException();
		}
	}
}
