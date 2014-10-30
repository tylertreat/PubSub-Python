import base64

from apiclient import errors
from apiclient.discovery import build
import httplib2

PUBSUB_SCOPE = "https://www.googleapis.com/auth/pubsub"


def get_client(project_id, credentials=None, service_account=None,
               private_key=None):
    """Return an instance of PubSubClient. Either AssertionCredentials or a
    service account and private key combination need to be provided in order to
    authenticate requests to BigQuery.

    Args:
        project_id: the BigQuery project id.
        credentials: an AssertionCredentials instance to authenticate requests
                     to BigQuery.
        service_account: the Google API service account name.
        private_key: the private key associated with the service account in
                     PKCS12 or PEM format.

    Returns:
        an instance of PubSubClient.
    """

    if not credentials and not (service_account and private_key):
        raise Exception('AssertionCredentials or service account and private'
                        'key need to be provided')

    pubsub_service = _get_pubsub_service(credentials=credentials,
                                         service_account=service_account,
                                         private_key=private_key)

    return PubSubClient(pubsub_service, project_id)


def _get_pubsub_service(credentials=None, service_account=None,
                        private_key=None):
    """Construct an authorized Pub/Sub service object."""

    assert credentials or (service_account and private_key)

    if not credentials:
        credentials = _credentials()(
            service_account, private_key, scope=PUBSUB_SCOPE)

    http = httplib2.Http()
    http = credentials.authorize(http)
    service = build('pubsub', 'v1beta1', http=http)

    return service


def _credentials():
    """Import and return SignedJwtAssertionCredentials class"""
    from oauth2client.client import SignedJwtAssertionCredentials

    return SignedJwtAssertionCredentials


class PubSubClient(object):

    def __init__(self, pubsub_service, project_id):
        self.pubsub = pubsub_service
        self.project_id = project_id

    def create_topic(self, name):
        """Create a topic if it doesn't exist. This is idempotent, meaning if
        the topic already exists, it has no effect.

        Args:
            name: the name of the topic to create.

        Raises:
            HttpError if the create failed.
        """

        name = self._full_topic_name(name)
        try:
            self.pubsub.topics().get(topic=name).execute()
        except errors.HttpError as e:
            if e.resp.status == 404:
                body = {'name': name}
                self.pubsub.topics().create(body=body).execute()
            else:
                raise

    def delete_topic(self, name):
        """Delete a topic. This is idempotent, meaning if the topic doesn't
        exist, it has no effect.

        Args:
            name: the name of the topic to delete.

        Raises:
            HttpError if the delete failed.
        """

        name = self._full_topic_name(name)
        try:
            self.pubsub.topics().delete(topic=name).execute()
        except errors.HttpError as e:
            if e.resp.status == 404:
                return
            raise

    def subscribe(self, name, topic, endpoint=None):
        """Create a subscription to a topic if it doesn't exist. This is
        idempotent, meaning if the subscription already exists, it has no
        effect.

        Args:
            name: the name of the subscription.
            topic: the name of the topic to subscribe to.
            endpoint: an endpoint the subscription should POST to when messages
                      are received.

        Raises:
            HttpError if the subscription creation failed.
        """

        name = self._full_subscription_name(name)
        try:
            self.pubsub.subscriptions().get(
                subscription=name).execute()
        except errors.HttpError as e:
            if e.resp.status == 404:
                body = {
                    'name': name,
                    'topic': self._full_topic_name(topic),
                    'pushConfig': {
                        'pushEndpoint': endpoint,
                    }
                }
                self.pubsub.subscriptions().create(body=body).execute()
            else:
                raise

    def unsubscribe(self, name):
        """Delete a subscription to a topic if it exists. This is idempotent,
        meaning if the subscription doesn't exist, it has no effect.

        Args:
            name: the name of the subscription to delete.

        Raises:
            HttpError is the subscription deletion failed.
        """

        name = self._full_subscription_name(name)
        try:
            self.pubsub.subscriptions().delete(subscription=name).execute()
        except errors.HttpError as e:
            if e.resp.status == 404:
                return
            raise

    def publish(self, topic, message):
        """Publish a message to a topic.

        Args:
            topic: the name of the topic to publish to.
            message: the body of the message as a string.


        Raises:
            HttpError if the publish failed.
        """

        topic = self._full_topic_name(topic)
        body = {
            'topic': topic,
            'message': {
                'data': base64.b64encode(message),
            }
        }
        self.pubsub.topics().publish(body=body).execute()

    def pull(self, subscription, block=False):
        """Pull a single message from a topic subscription.

        Args:
            subscription: the name of the subscription to pull from.
            block: bool indicating if the pull should block until a message is
                   available or a timeout occurs. If false, pull will return
                   immediately.

        Returns:
            string containing the message data or None if no message was
            retrieved.

        Raises:
            HttpError if the pull failed.
        """

        subscription = self._full_subscription_name(subscription)
        body = {'subscription': subscription, 'returnImmediately': not block}
        resp = self.pubsub.subscriptions().pull(body=body).execute()
        message = resp.get('pubsubEvent').get('message')

        if message:
            ack_id = resp.get('ackId')
            ack_body = {'subscription': subscription, 'ackId': [ack_id]}
            self.pubsub.subscriptions().acknowledge(body=ack_body).execute()
            return base64.b64decode(message.get('data'))

        return None

    def _full_topic_name(self, name):
        return '/topics/%s/%s' % (self.project_id, name)

    def _full_subscription_name(self, name):
        return '/subscriptions/%s/%s' % (self.project_id, name)

