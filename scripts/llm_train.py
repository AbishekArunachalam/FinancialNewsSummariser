from datasets import load_dataset
import pandas as pd
import evaluate
from transformers import AutoModelForSeq2SeqLM, PegasusForConditionalGeneration, BartForConditionalGeneration
from transformers import AutoTokenizer, logging, PegasusTokenizer, BartTokenizer
from transformers import GenerationConfig
from transformers import tokenization_utils_base

class TrainLLM():

    @classmethod
    def __init__(self):
        logging.set_verbosity_error()
        self.eval_data = load_dataset("cnn_dailymail", "3.0.0", split="validation")
                                           
        t5_model_name = 'google/flan-t5-base'
        bart_model_name = 'facebook/bart-large-cnn'
        pegasus_model_name = 'google/pegasus-cnn_dailymail'

        self.auto_tokenizer = AutoTokenizer.from_pretrained(t5_model_name, use_fast=True)
        self.bart_tokenizer = BartTokenizer.from_pretrained(bart_model_name, use_fast=True)
        self.pegasus_tokenizer = PegasusTokenizer.from_pretrained(pegasus_model_name, use_fast=True)

        self.t5_model = AutoModelForSeq2SeqLM.from_pretrained(t5_model_name)
        self.bart_model = BartForConditionalGeneration.from_pretrained(bart_model_name)
        self.pegasus_model = PegasusForConditionalGeneration.from_pretrained(pegasus_model_name)


    @classmethod
    def tokenise_text(self, valid_data, tokenizer, model_obj) -> list:
        """
        Args
        news_article: text of a financial news article

        Returns:
        tokenized_news: encoded pytorch tensors
        """
        generation_config = GenerationConfig(max_new_tokens=60, do_sample=True, temperature=0.9)
        lmm_summary = list()
        highlights = list()
        for i, data in enumerate(valid_data):
            input_article = tokenizer(data['article'], return_tensors='pt')
            output = tokenizer.decode(
                        model_obj.generate(
                            input_article["input_ids"], 
                            generation_config= generation_config,
                            max_new_tokens=60,
                        )[0], 
                        skip_special_tokens=True
                    )
            lmm_summary.append(output)
            highlights.append(data['highlights'])

        return list(zip(highlights,lmm_summary))
    

    @classmethod
    def evaluate_model(self, summaries):

        summary_df = pd.DataFrame(summaries, columns = ['human_baseline_summaries', 'llm_model_summaries'])
        human_baseline_summaries = summary_df['human_baseline_summaries'].values
        llm_model_summaries = summary_df['llm_model_summaries'].values

        rouge = evaluate.load('rouge')

        eval_results = rouge.compute(
            predictions=llm_model_summaries,
            references=human_baseline_summaries[0:len(llm_model_summaries)],
            use_aggregator=True,
            use_stemmer=True,
        )
        print(type(eval_results))

        return eval_results


    @classmethod
    def t5_evaluate(self, valid_data) -> list:
        """
        Args
        token_txt: encoded tokens of pytorch tensors

        Returns:
        summary_txt: decoded summary of news article
        """
        lmm_summaries = self.tokenise_text(valid_data, self.auto_tokenizer, self.t5_model)
        eval_results = self.evaluate_model(lmm_summaries)

        return eval_results
    
    
    @classmethod
    def bart_evaluate(self, valid_data) -> list:
        """
        Args
        token_txt: encoded tokens of pytorch tensors

        Returns:
        summary_txt: decoded summary of news article
        """
        lmm_summaries = self.tokenise_text(valid_data, self.bart_tokenizer, self.bart_model)
        eval_results = self.evaluate_model(lmm_summaries)

        return eval_results


    @classmethod
    def pegasus_evaluate(self, valid_data) -> list:
        """
        Args
        token_txt: encoded tokens of pytorch tensors

        Returns:
        summary_txt: decoded summary of news article
        """
        lmm_summaries = self.tokenise_text(valid_data, self.pegasus_tokenizer, self.pegasus_model)
        eval_results = self.evaluate_model(lmm_summaries)

        return eval_results
    
#Alpaca API
# Financial BERT and BART model, Pegasus model, T5 (smaller model)

if __name__ == "__main__":
    obj = TrainLLM()
    valid_data = obj.eval_data
    print(f"T5 model rouge score: {obj.t5_evaluate(valid_data)}")

    #print(f"BART model rouge score: {obj.bart_evaluate(valid_data)}")
    #print(f"Pegasus model rouge score: {obj.pegasus_evaluate(valid_data)}")