package zx.soft.hdfs.images.lab;

/**
 * This exception is thrown when an operation is performed on a file that purportedly of an image
 * type, but the <ImageType>Image class has trouble parsing it.
 */
public class ImageFormatException extends Exception {

	/** Creates an ImageFormatException with the given message string. */
	public ImageFormatException(String message) {
		super(message);
	}

	/** Creates an ImageFormatException with a message string and the given
	    cause. */
	public ImageFormatException(String message, Throwable cause) {
		super(message, cause);
	}

	/** Creates an ImageFormatException from the given cause. */
	public ImageFormatException(Throwable cause) {
		super(cause);
	}

}
