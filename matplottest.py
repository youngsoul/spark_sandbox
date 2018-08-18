import matplotlib.pyplot as plt
from wordcloud import WordCloud


word_count_dict = dict(zip(['word','test', 'matplot', 'works'], [2, 5, 10, 4]))

print(word_count_dict)

if len(word_count_dict) > 0:
    wordcloud = WordCloud().generate_from_frequencies(word_count_dict)  # WordCloud().generate(text)

    # Display the generated image:
    # the matplotlib way:
    plt.figure(figsize=(16, 10))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")

    plt.show()
