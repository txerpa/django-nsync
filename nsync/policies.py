from django.db import transaction


class BasicSyncPolicy:
    def __init__(self, actions):
        self.actions = actions

    def execute(self):
        for action in self.actions:
            action.execute()


class TransactionSyncPolicy:
    def __init__(self, policy):
        self.policy = policy

    def execute(self):
        with transaction.atomic():
            self.policy.execute()

