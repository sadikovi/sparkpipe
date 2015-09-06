package org.sparkpipe.util.config

import scala.io.Source
import scala.util.{Try, Success, Failure}
import java.io.{InputStream, File}

/**
 * Mode for duplicate keys.
 * `Override` - overrides existing key.
 * `Ignore` - ignores newer key.
 * `Throw` - throws exception saying that key already exists.
 */
object DuplicateMode extends Enumeration {
    val Override, Ignore, Throw = Value
}

/** Configuration parser */
private[config] class Config(private val iter: Iterator[String], val mode: DuplicateMode.Value) {
    // Configuration is always scanned sequentially from beginning of the file. This allows us to
    // know the current section. Each key - value pair is stored as hashkey - value. Hashkey is a
    // some combination defined in `hashkey` method.
    // Configuration also checks for a valid file lines and will throw exception if file contains
    // lines that cannot be parsed as section / key-value / comment / new line
    private val SECTION_PATTERN = """^\s*\[([\w-]+)\]\s*$""".r("section")
    private val KEYVALUE_PATTERN = """^([\w-]+)\s*=\s*([^"\s]*|"[^\r\n]*")$""".r("key", "value")
    private val COMMENT_PATTERN = """^#.*""".r
    private val NEWLINE_PATTERN = """^\s*$""".r

    private val defaultSection: String = "@GLOBAL"
    // all the pairs scanned for the input
    private val pairs: Map[String, String] = load(iter, mode)

    /** Returns configuration with default key rule */
    def this(iter: Iterator[String]) = this(iter, DuplicateMode.Override)

    /**
     * Load configuration. This is where all the fun happens.
     * @param iter iterator of strings
     * @param mode rule to insert key
     * @return map of key/value pairs from that configuration
     */
    private def load(iter: Iterator[String], mode: DuplicateMode.Value): Map[String, String] = {
        var map: Map[String, String] = Map()
        var currentSection: String = defaultSection
        // zip iterator with index, so we know the line number
        iter.zipWithIndex.foreach(
            bucket => {
                // get line and line number
                val (line, lineNumber) = bucket
                // strip all the trailing spaces
                val prepared = line.trim()
                // do match
                prepared match {
                    case section if SECTION_PATTERN.findFirstMatchIn(section).nonEmpty => {
                        val m = SECTION_PATTERN.findFirstMatchIn(section).get
                        currentSection = m.group("section")
                    }
                    case keyvalue if KEYVALUE_PATTERN.findFirstMatchIn(prepared).nonEmpty => {
                        val m = KEYVALUE_PATTERN.findFirstMatchIn(prepared).get
                        val (key, value) = (m.group("key"), m.group("value"))
                        map = storeKeyInMap(map, key, currentSection, value,
                            _.stripPrefix("\"").stripSuffix("\""), mode)
                    }
                    case comment if COMMENT_PATTERN.findFirstMatchIn(prepared).nonEmpty => {}
                    case newline if NEWLINE_PATTERN.findFirstMatchIn(prepared).nonEmpty => {}
                    case _ => throw new Exception("Wrong format on line " + (lineNumber + 1) +
                        ": " + line.slice(0, 3) + "...")
                }
            }
        )
        // return map of key - section, value
        map
    }

    /**
     * Hash key for key, section pair.
     * @param key key
     * @param section section
     * @return hash key
     */
    private def hashkey(key:String, section:String): String = "@" + key + "###" + section

    /**
     * Method stores key in map provided, as map is mutable.
     * @param map map to store key-value
     * @param key key
     * @param section section to store value for
     * @param value value
     * @param modifier modifier for value before storing in map
     * @param mode enforcing rule to store key-value
     * @return updated pairs map
     */
    private def storeKeyInMap(
        map: Map[String, String],
        key: String,
        section: String,
        value: String,
        modifier: String => String,
        mode: DuplicateMode.Value
    ): Map[String, String] = {
        val hash = hashkey(key, section)
        // sadikovi: remove surrounding quotes for escaped values
        val editedValue = modifier(value)
        if (map.contains(hash)) {
            mode match {
                case DuplicateMode.Override => map + (hash -> editedValue)
                case DuplicateMode.Ignore => map
                case DuplicateMode.Throw => throw new Exception("Key " + key + " for section " +
                    section + " already exists")
            }
        } else {
            map + (hash -> editedValue)
        }
    }

    /**
     * Returns value as Option. General method for Config.
     * @param map map of key-value pairs
     * @param key key
     * @param section section for value
     * @return value for specified key, section
     */
    private def getKeyFromMap(
        map: Map[String, String],
        key:String,
        section:String
    ): Option[String] = {
        val hash = hashkey(key, section)
        map.get(hash)
    }

    /** For testing purposes only */
    private[config] def allPairs: Map[String, String] = pairs

    /** Private generic getter */
    private def get(key: String, section: String): Option[String] =
        getKeyFromMap(pairs, key, section)

    /**
     * Safe conversion from String to some type T.
     * @param value value to convert
     * @param func conversion function
     * @return option of convertion type
     */
    private def convert[T](
        value: String,
        func: String => T
    ): Option[T] = Try(func(value)) match {
        case Success(a) => Option(a)
        case Failure(e) => None
    }

    /**
     * Get method for any T other than String types. `func` is function to convert to type T.
     * @param key key
     * @param section section for value
     * @param func conversion function
     * @return option of type T
     */
    private def getSome[T](
        key: String,
        section: String,
        func: String => T
    ): Option[T] = get(key, section) match {
        case Some(s) => convert(s, func)
        case None => None
    }

    // API
    /** true, if configuration could not find any key-value pairs */
    def isEmpty: Boolean = pairs.isEmpty

    def getString(key: String, section: String = defaultSection): Option[String] =
        getSome(key, section, _.toString)

    def getInt(key: String, section: String = defaultSection): Option[Int] =
        getSome(key, section, _.toInt)

    def getDouble(key: String, section: String = defaultSection): Option[Double] =
        getSome(key, section, _.toDouble)

    def getBoolean(key: String, section: String = defaultSection): Option[Boolean] =
        getSome(key, section, _.toBoolean)
}

/** Configuration factory */
object Config {

    /**
     * Creates configuration from file path.
     * @param path file path
     * @param mode parsing rule for duplicates, default is override
     * @return loaded configuration
     */
    def fromPath(path: String, mode: DuplicateMode.Value=DuplicateMode.Override): Config = {
        val iter = Source.fromFile(path).getLines()
        new Config(iter, mode)
    }

    /**
     * Creates configuration from `java.io.File` object.
     * @param file File instance
     * @param mode parsing rule for duplicates, default is override
     * @return loaded configuration
     */
    def fromFile(file: File, mode: DuplicateMode.Value=DuplicateMode.Override): Config = {
        val iter = Source.fromFile(file).getLines()
        new Config(iter, mode)
    }

    /**
     * Creates configuration from stream.
     * @param stream InputStream instance
     * @param mode parsing rule for duplicates, default is override
     * @return loaded configuration
     */
    def fromStream(stream: InputStream, mode: DuplicateMode.Value=DuplicateMode.Override): Config = {
        val iter = Source.fromInputStream(stream).getLines()
        new Config(iter, mode)
    }
}
