from rec_model import RecommendSystem

rec_sys = RecommendSystem()
rec_sys.restore_model()
print(rec_sys.recommend(rec_sys.users[0]))
