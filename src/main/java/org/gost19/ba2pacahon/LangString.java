package org.gost19.ba2pacahon;

public class LangString
{
	String text_ru = null;
	String text_en = null;

	public static LangString parse(String in) throws Exception
	{
		LangString res = new LangString();

		if (in.charAt(0) == '@')
		{
			String[] labels = in.split("@");

			for (String label : labels)
			{
				if (label.length() > 3)
				{
					int start_text = label.indexOf("{");
					if (start_text < 0)
						throw new Exception("invalid format lang string");
					String lang = label.substring(0, start_text);
					String text = label.substring(start_text + 1, label.length() - 1);
					if (lang.equals("en"))
					{
						res.text_en = text;
					} else if (lang.equals("ru"))
					{
						res.text_ru = text;
					}

				}
			}
		} else
		{
			res.text_ru = in;
		}

		return res;
	}

}
