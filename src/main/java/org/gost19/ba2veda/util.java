package org.gost19.ba2veda;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.CharacterIterator;
import java.text.SimpleDateFormat;
import java.text.StringCharacterIterator;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.ByteArrayPartSource;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.StringPart;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.io.output.ByteArrayOutputStream;

public class util
{
	private static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.sss");
	private static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	private static SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd");

	public static String escape(String string)
	{

		if (string != null)
		{
			StringBuilder sb = new StringBuilder();

			char c = ' ';

			for (int i = 0; i < string.length(); i++)
			{

				c = string.charAt(i);

				if (c == '\n')
				{
					sb.append("\\n");
				} else if (c == '\r')
				{
					sb.append("\\r");
				} else if (c == '\\')
				{
					sb.append("\\\\");
				} else if (c == '"')
				{
					sb.append("\\\"");
				} else if (c == '\t')
				{
					sb.append("\\t");
				} else
				{
					sb.append(c);
				}
			}
			return sb.toString();
		} else
		{
			return "";
		}
	}

	final static String _None = "NONE";
	final static String _Ru = "RU";
	final static String _En = "EN";

	public static void addProperty(StringBuffer sb, String name, Resources rcs)
	{
		if (rcs.resources.size() == 0)
			return;

		int ll = sb.length() - 1;
		if (ll > 0)
		{
			while (sb.charAt(ll) == ' ')
				ll--;

			char ch = sb.charAt(ll);

			if (ch == '"' || ch == '}' || ch == ']')
				sb.append(", ");
		}

		sb.append('"');
		sb.append(name);
		sb.append("\":[");

		int cc = 0;
		for (Resource rc : rcs.resources)
		{
			if (cc > 0)
				sb.append(',');

			sb.append("{\"data\":");

			if (rc.type == Resource._String || rc.type == Resource._Uri)
			{
				sb.append('"');
				sb.append(util.forJSON((String) rc.data));
				// sb.append((String) rc.data);
				sb.append('"');
			} else if (rc.type == Resource._Bool || rc.type == Resource._Integer || rc.type == Resource._Decimal)
			{
				sb.append((String) rc.data);
			} else if (rc.type == Resource._Datetime)
			{
				sb.append('"');
				sb.append((String) rc.data);
				sb.append('"');
			}

			sb.append(", \"type\":");
			sb.append(rc.type);

			if (rc.lang.equals(_None) == false)
			{
				sb.append(", \"lang\":\"");
				sb.append(rc.lang);
				sb.append('"');
			}

			sb.append("}");
			cc++;
		}
		sb.append("]");
	}

	/**
	 * Transforms String to Date.
	 * 
	 * @param date
	 * @param time
	 * @return XMLGregorianCalendar
	 */
	public static Date string2date(String date)
	{
		date = date.replace('T', ' ');
		date = date.substring(0, date.indexOf('+'));
		GregorianCalendar gcal = new GregorianCalendar();
		try
		{
			if (date.length() < 22)
				gcal.setTime(sdf2.parse(date));
			else
				gcal.setTime(sdf1.parse(date));
			return gcal.getTime();
		} catch (Exception ex)
		{
			ex.hashCode();
		}
		return null;
	}

	public static String date2string(Date date)
	{
		StringBuilder sb = new StringBuilder(sdf2.format(date));
		sb.setCharAt(10, 'T');
		return sb.toString();
	}

	public static String date2_short_string(Date date)
	{
		StringBuilder sb = new StringBuilder(sdf3.format(date));
		return sb.toString();
	}

	public static String excutePost(String targetURL, String urlParameters)
	{
		URL url;
		HttpURLConnection connection = null;
		try
		{
			// Create connection
			url = new URL(targetURL);
			connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("POST");
			connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

			connection.setRequestProperty("Content-Length", "" + Integer.toString(urlParameters.getBytes().length));
			connection.setRequestProperty("Content-Language", "en-US");
			connection.setRequestProperty("charset", "utf-8");

			connection.setUseCaches(false);
			// connection.setDoInput(true);
			connection.setDoOutput(true);

			// Send request
			DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
			wr.writeBytes(urlParameters);
			wr.flush();
			wr.close();

			int responseCode = connection.getResponseCode();
			System.out.println("\nSending 'POST' request to URL : " + url);
			System.out.println("Post parameters : " + urlParameters);
			System.out.println("Response Code : " + responseCode);

			// Get Response
			InputStream is = connection.getInputStream();
			BufferedReader rd = new BufferedReader(new InputStreamReader(is));
			String line;
			StringBuffer response = new StringBuffer();
			while ((line = rd.readLine()) != null)
			{
				response.append(line);
				response.append('\r');
			}
			rd.close();
			return response.toString();

		} catch (Exception e)
		{

			e.printStackTrace();
			return null;

		} finally
		{

			if (connection != null)
			{
				connection.disconnect();
			}
		}
	}
	
	public static String excuteGet(String targetURL)
	{
		URL url;
		HttpURLConnection connection = null;
		try
		{
			// Create connection
			url = new URL(targetURL);
			connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");

			connection.setUseCaches(false);
			connection.setDoOutput(true);

			// Get Response
			InputStream is = connection.getInputStream();
			BufferedReader rd = new BufferedReader(new InputStreamReader(is));
			String line;
			StringBuffer response = new StringBuffer();
			while ((line = rd.readLine()) != null)
			{
				response.append(line);
				response.append('\r');
			}
			rd.close();
			return response.toString();

		} catch (Exception e)
		{

			e.printStackTrace();
			return null;

		} finally
		{

			if (connection != null)
			{
				connection.disconnect();
			}
		}
	}

	public static int excutePut(String targetURL, String urlParameters)
	{
		// System.out.println("Post parameters : " + urlParameters);

		URL url;
		HttpURLConnection connection = null;
		try
		{
			// Create connection
			url = new URL(targetURL);
			connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("PUT");
			// connection.setRequestProperty("Content-Type",
			// "application/x-www-form-urlencoded");

			connection.setRequestProperty("Content-Type", "application/json");
			connection.setRequestProperty("Accept", "application/json");
			connection.setRequestProperty("charset", "utf-8");

			connection.setUseCaches(false);
			// connection.setDoInput(true);
			connection.setDoOutput(true);

			// Send request
			DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(wr, "UTF-8"));
			writer.write(urlParameters);
			writer.close();
			// wr.writeBytes(urlParameters);
			wr.flush();
			wr.close();

			int responseCode = connection.getResponseCode();

			if (responseCode != 200)
			{
				System.out.println("\nSending 'PUT' request to URL : " + url);
				System.out.println("Post parameters : " + urlParameters);
				System.out.println("Response Code : " + responseCode);
			}

			return responseCode;

		} catch (java.net.NoRouteToHostException e)
		{
			try
			{
				if (connection != null)
				{
					connection.disconnect();
					connection = null;
				}

				// e.printStackTrace();
				Thread.currentThread().sleep(1000);
				System.out.println("retry");
				return excutePut(targetURL, urlParameters);
			} catch (Exception ex)
			{
				System.out.println("Post parameters : " + urlParameters);
				return -1;
			}
		} catch (Exception e)
		{

			e.printStackTrace();
			return -1;

		} finally
		{

			if (connection != null)
			{
				connection.disconnect();
			}
		}
	}

	private static ByteArrayPartSource getResourceAsPartSource(String fileName, InputStream in) throws IOException
	{
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024];
		while (true)
		{
			int rc = in.read(buffer);
			if (rc < 0)
				break;
			out.write(buffer, 0, rc);
		}
		in.close();
		return new ByteArrayPartSource(fileName, out.toByteArray());
	}

	public static int upload(String uri, InputStream in, String fileName, String targetDir) throws Exception
	{
		ByteArrayPartSource source = getResourceAsPartSource(fileName, in);

		PostMethod filePost = new PostMethod(uri);

		filePost.getParams().setBooleanParameter(HttpMethodParams.USE_EXPECT_CONTINUE, true);
		filePost.addParameter("url", targetDir);
		filePost.addParameter("fileName", fileName);

		FilePart part = new FilePart("file", source);
		part.setContentType("multipart/form-data");
		part.setTransferEncoding("binary");

		StringPart sp = new StringPart("path", targetDir);

		Part[] parts =
		{ part, sp };

		filePost.setRequestEntity(new MultipartRequestEntity(parts, filePost.getParams()));

		HttpClient client = new HttpClient();
		client.getHttpConnectionManager().getParams().setConnectionTimeout(5000);

		int status = client.executeMethod(filePost);
		if (status == HttpStatus.SC_OK)
		{
			System.out.println("uploaded file:" + targetDir + "/" + fileName);
			return 0;
		} else
		{
			System.out.println("responseString:" + filePost.getResponseBodyAsString());
			return -1;
		}

	}

	public static String forJSON(String input)
	{
		if (input == null || input.isEmpty())
		{
			return "";
		}
		int len = input.length();
		// сделаем небольшой запас, чтобы не выделять память потом
		final StringBuilder result = new StringBuilder(len + len / 4);
		final StringCharacterIterator iterator = new StringCharacterIterator(input);
		char ch = iterator.current();
		while (ch != CharacterIterator.DONE)
		{
			if (ch == '\n')
			{
				result.append("\\n");
			} else if (ch == '\r')
			{
				result.append("\\r");
			}/*
				* else if (ch == '\'') { result.append("\\\'"); }
				*/else if (ch == '"')
			{
				result.append("\\\"");
			} else
			{
				result.append(ch);
			}
			ch = iterator.next();
		}
		return result.toString();
	}

}