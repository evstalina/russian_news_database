import re
import pandas as pd
import spacy
from spacy import displacy
import warnings
warnings.filterwarnings("ignore")

nlp = spacy.load("ru_core_news_sm")


class NER_dashboard:
    def __init__(self):
        self.colors = {}
        self.patterns = []
        self.nlp = spacy.load("ru_core_news_sm")
        self.ruler = self.nlp.add_pipe("entity_ruler")
        self.NUM_PATTERNS = []
        self.NUM_PATTERNS.append(re.compile(r"(^|\s)(\d+)[./-](\d+)[./-](\d+)($|\s)"))
        self.NUM_PATTERNS.append(re.compile(r"(^|\s)(\d+)\sг[А-я.]{1,6}($|\s)"))
        self.NUM_PATTERNS.append(
            re.compile(r"(^|\s)(\d){0,2}\s{0,1}(январ[ьея]|феврал[ьея]|март[еа]?|апрел[ьея]|ма[йея]|ию[нл][яье]|"
                       r"август[еа]?|(?:сент|окт|но|дек)[ая]бр[яье])\s{0,1}(\d){0,4}(\s|-){0,1}(г[А-я.]){0,6}($|\s)",
                       flags=re.I))
        self.colors['ДАТА'] = '#E8F6F3'
        self.gs_preproc(pd.read_excel('data/ner_aux/gs.xlsx'))

    def gs_preproc(self, gs):
        cols = ['#17A589', '#F1C40F', '#C39BD3']
        for i, comp in enumerate(gs.component.unique()):
            gs_temp = gs[gs.component == comp]
            patterns_words = [[{'LEMMA': word}] for word in gs_temp.word]
            for pattern in patterns_words:
                self.patterns.append({"label": comp, "pattern": pattern})
            self.colors[comp] = cols[i]
        self.ruler.add_patterns(self.patterns)

    def nlp_func(self, row):
        # эти костыли для того, чтобы добавить поверх необходимые spans
        doc = self.nlp(str(row['news_for_reading']))  # Expected a string or 'Doc' as input, but got: <class 'float'>.
        # #удаляю ненужные ner(ORG+FIO)
        # 4317129024397789502 - ФИО
        # 383 - ORG
        # 385 - LOC
        ents_show = []
        for entity in doc.ents:
            if entity.label not in [4317129024397789502, 383]:
                ents_show.append({"start": entity.start_char,
                                 "end": entity.end_char, "label": entity.label_})

        # re.I игнор upper lower cases
        for NUM_PATTERN in self.NUM_PATTERNS:
            for match in re.finditer(NUM_PATTERN, doc.text):
                start, end = (match.span()[0], match.span()[1])
                ents_show.append({"start": start, "end": end, "label": 'ДАТА'})

        # иначе если будет start < посл., то он будет считать с последнего char_end + start
        ents_show.sort(key=lambda x: x['start'])
        # убираю entity, которые частично друг друга дублируют
        for i, elem in enumerate(ents_show):
            if i > 0 and elem['start'] <= ents_show[i-1]['end']:
                ents_show.remove(elem)
        show = [{'text': doc.text, 'ents': ents_show, 'title': None}]
        # jupyter=True,
        return displacy.render(show, style="ent", manual=True, options={'colors': self.colors})
