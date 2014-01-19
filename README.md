<h1> ArcmainCompressor </h1>

<strong>MR job that compresses and uncompresses binary files into gzip or bzip2.</strong>

<b>Usage</b>: 
FileCompressorJob < inputPath > < outputPath > < # mappers > < compressionCodec > < GZIPBufferSize (default 8192; optional) >

<strong>Example:</strong> hadoop jar ArcmainMapper-0.0.1-SNAPSHOT.jar com.cloudera.sa.ArcmainMapper.FileCompressorJob input_files output_files 10 gzip 

<strong>Usage:</strong>
FileUnCompressorJob < inputPath > < outputPath > < # mappers >

<strong>Example:</strong> hadoop jar ArcmainMapper-0.0.1-SNAPSHOT.jar com.cloudera.sa.ArcmainMapper.FileUnCompressorJob input_files output_files 10

<strong>Description</strong><br>
1. Read files from a directory and compress using specified codec (gzip | bzip2). <br>
2. Map-Only job that reads / writes directly to HDFS. <br>
3. Utilizes custom input format to read files in the directory and distribute them to the mappers.<br>
4. Supports decompressing gzip or bzip2. <br>
5. Files must end in .gz or .bz2 for Decompress to function properly.

