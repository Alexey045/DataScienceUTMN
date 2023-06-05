import numpy as np
import implicit
from scipy.sparse import csr_matrix
from click import click
import pandas as pd
import pickle


class RecommendSystem:
    def __init__(self):
        self.model = None
        self.users_dict = None
        self.objects_dict = None
        self.users = None
        self.objects = None
        self.item_users = None
        self.item_users_test = None

    def train(self) -> None:
        data_array = np.array(click.client.execute(
            "SELECT toString(UserID) as UserID, ObjectID, "
            "COUNT(DISTINCT toStartOfMinute(ViewDate)) as TotalViews "
            "FROM views2 "
            "WHERE ObjectID > 0 "
            "AND ObjectClass = 'flats' "
            "AND  ViewDate >(NOW() - toIntervalWeek(10)) "
            "GROUP BY  UserID, ObjectID "
            "HAVING TotalViews < 80 "))

        df = pd.DataFrame(data_array, columns=["uid", "oid", "totalViews"])

        self.users = df.uid.unique()
        self.objects = df.oid.unique()

        self.users_dict = {}
        for uid_index, uid in enumerate(self.users):
            self.users_dict[uid] = uid_index

        self.objects_dict = {}
        for oid_index, oid in enumerate(self.objects):
            self.objects_dict[oid] = oid_index

        shape = (self.users.size, self.objects.size)

        df.uid = df.uid.apply(lambda x: self.users_dict[x])
        df.oid = df.oid.apply(
            lambda y: self.objects_dict[y])
        df.totalViews = df.totalViews.apply(
            lambda z: int(z) + (int(z) - 1) * 2)

        self.item_users = csr_matrix(
            (df.totalViews, (df.uid, df.oid)), shape=shape)

        model = implicit.als.AlternatingLeastSquares(factors=50)
        model.fit(self.item_users)  # show_progress=False
        self.model = model

    def recommend(self, test_user: str) -> list[int]:
        test_user_id = self.users_dict[test_user]
        recommendations = self.model.recommend(test_user_id,
                                               self.item_users[test_user_id])
        result = [int(r) for r in recommendations[0]]
        return result

    def store_model(self) -> None:
        model_data = {
            "model": self.model,
            "users_dict": self.users_dict,
            "objects_dict": self.objects_dict,
            "users": self.users,
            "objects": self.objects,
            "csr_matrix": self.item_users
        }
        with open("../model_data.pickle", "wb") as file:
            pickle.dump(model_data, file)

    def restore_model(self) -> None:
        with open("../model_data.pickle", "rb") as file:
            model_data = pickle.load(file)
        self.model = model_data['model']
        self.users_dict = model_data['users_dict']
        self.objects_dict: dict = model_data['objects_dict']
        self.users = model_data['users']
        self.objects = model_data['objects']
        self.item_users = model_data['csr_matrix']