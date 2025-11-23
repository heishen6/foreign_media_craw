from langchain_openai import ChatOpenAI
import logging

class Translator:
    def __init__(self, config):
        # Use ChatOpenAI for DeepSeek (OpenAI compatible)
        # Modern LangChain v1 usage via integration package
        self.model = ChatOpenAI(
            model=config["model_name"],
            temperature=config["temperature"],
            openai_api_key=config.get("api_key"),
            openai_api_base=config.get("base_url"),
            timeout=config.get("timeout", 10),
            max_tokens=config.get("max_tokens", 1000)
        )
        
    def translate_title(self, title):
        try:
            messages = [
                ("system", "You are a professional translator."),
                ("user", f"Please translate the following news title to Chinese (Simplified). Only output the translated title, no other text.\n\nTitle: {title}")
            ]
            response = self.model.invoke(messages)
            return response.content.strip()
        except Exception as e:
            logging.error(f"Translation error: {e}")
            return title

    def translate_summary(self, text):
        try:
            if not text:
                return ""
            # Limit text length to save tokens/time if it's too long, though user wants "summary" which is usually short-ish.
            # FT summary is usually short. FJ data tables might be long.
            # If it's a data table (e.g. "S&P 500: ..."), translation might mangle it or be unnecessary.
            # But user asked for translation.
            messages = [
                ("system", "You are a professional translator."),
                ("user", f"Please translate the following news summary/description to Chinese (Simplified). Keep it concise. Only output the translated text.\n\nText: {text[:500]}")
            ]
            response = self.model.invoke(messages)
            return response.content.strip()
        except Exception as e:
            logging.error(f"Summary translation error: {e}")
            return text
