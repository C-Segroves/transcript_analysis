
from youtube_transcript_api import YouTubeTranscriptApi

test_=YouTubeTranscriptApi().fetch('B0BlQkOatX8', languages=['en'], preserve_formatting=True)

print(test_)