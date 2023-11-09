package be.icteam.adobe.analytics.datafeed.util

import org.apache.spark.sql.types.{StringType, StructField, StructType}

case object MobileAttributes {

  val schema: StructType = StructType(Array(
    StructField("mobile_id", StringType),
    StructField("Manufacturer", StringType),
    StructField("Device", StringType),
    StructField("Device_Type", StringType),
    StructField("Operating_System", StringType),
    StructField("Diagonal_Screen_Size", StringType),
    StructField("Screen_Height", StringType),
    StructField("Screen_Width", StringType),
    StructField("Cookie_Support", StringType),
    StructField("Color_Depth", StringType),
    StructField("MP3_Audio_Support", StringType),
    StructField("AAC_Audio_Support", StringType),
    StructField("AMR_Audio_Support", StringType),
    StructField("Midi_Monophonic_Audio_Support", StringType),
    StructField("Midi_Polyphonic_Audio_Support", StringType),
    StructField("QCELP_Audio_Support", StringType),
    StructField("GIF87_Image_Support", StringType),
    StructField("GIF89a_Image_Support", StringType),
    StructField("PNG_Image_Support", StringType),
    StructField("JPG_Image_Support", StringType),
    StructField("3GPP_Video_Support", StringType),
    StructField("MPEG4_Video_Support", StringType),
    StructField("3GPP2_Video_Support", StringType),
    StructField("WMV_Video_Support", StringType),
    StructField("MPEG4_Part_2_Video_Support", StringType),
    StructField("Stream_MP4_AAC_LC_Video_Support", StringType),
    StructField("Stream_3GP_H264_Level_10b_Video_Support", StringType),
    StructField("Stream_3GP_AAC_LC_Video_Support", StringType),
    StructField("3GP_AAC_LC_Video_Support", StringType),
    StructField("Stream_MP4_H264_Level_11_Video_Support", StringType),
    StructField("Stream_MP4_H264_Level_13_Video_Support", StringType),
    StructField("Stream_3GP_H264_Level_12_Video_Support", StringType),
    StructField("Stream_3GP_H264_Level_11_Video_Support", StringType),
    StructField("Stream_3GP_H264_Level_10_Video_Support", StringType),
    StructField("Stream_3GP_H264_Level_13_Video_Support", StringType),
    StructField("3GP_AMR_NB_Video_Support", StringType),
    StructField("3GP_AMR_WB_Video_Support", StringType),
    StructField("MP4_H264_Level_11_Video_Support", StringType),
    StructField("3GP_H263_Video_Support", StringType),
    StructField("MP4_H264_Level_13_Video_Support", StringType),
    StructField("Stream_3GP_H263_Video_Support", StringType),
    StructField("Stream_3GP_AMR_WB_Video_Support", StringType),
    StructField("3GP_H264_Level_10b_Video_Support", StringType),
    StructField("MP4_ACC_LC_Video_Support", StringType),
    StructField("Stream_3GP_AMR_NB_Video_Support", StringType),
    StructField("3GP_H264_Level_10_Video_Support", StringType),
    StructField("3GP_H264_Level_13_Video_Support", StringType),
    StructField("3GP_H264_Level_11_Video_Support", StringType),
    StructField("3GP_H264_Level_12_Video_Support", StringType),
    StructField("Stream_HTTP_Live_Streaming_Video_Support", StringType)
  ))

}
