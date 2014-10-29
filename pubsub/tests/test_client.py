import unittest

from apiclient import errors
import mock

from pubsub import client


class TestGetClient(unittest.TestCase):

    def test_no_credentials(self):
        """Ensure an Exception is raised when no credentials are provided."""

        self.assertRaises(Exception, client.get_client, 'foo', 'bar')

    @mock.patch('pubsub.client._credentials')
    @mock.patch('pubsub.client.build')
    def test_initialize(self, mock_build, mock_return_cred):
        """Ensure that a PubSubClient is initialized and returned."""
        from pubsub.client import PUBSUB_SCOPE

        mock_cred = mock.Mock()
        mock_http = mock.Mock()
        mock_cred.return_value.authorize.return_value = mock_http
        mock_pubsub = mock.Mock()
        mock_build.return_value = mock_pubsub
        key = 'key'
        service_account = 'account'
        project_id = 'project'
        mock_return_cred.return_value = mock_cred

        pubsub_client = client.get_client(
            project_id, service_account=service_account, private_key=key)

        mock_return_cred.assert_called_once_with()
        mock_cred.assert_called_once_with(service_account, key,
                                          scope=PUBSUB_SCOPE)
        mock_cred.authorize.assert_called_once()
        mock_build.assert_called_once_with('pubsub', 'v1beta1', http=mock_http)
        self.assertEquals(mock_pubsub, pubsub_client.pubsub)
        self.assertEquals(project_id, pubsub_client.project_id)


class TestCreateTopic(unittest.TestCase):

    def setUp(self):
        self.project_id = 'project'
        self.mock_pubsub = mock.Mock()
        self.client = client.PubSubClient(self.mock_pubsub, self.project_id)

    def test_topic_exists(self):
        """Ensure that nothing happens if the topic already exists."""

        mock_topics = mock.Mock()
        mock_topic = mock.Mock()
        mock_topics.get.return_value = mock_topic
        self.mock_pubsub.topics.return_value = mock_topics

        self.client.create_topic('foo')

        mock_topics.get.assert_called_once_with(topic='/topics/project/foo')
        mock_topic.execute.assert_called_once_with()

    def test_topic_doesnt_exist(self):
        """Ensure that the topic is created if it doesn't exist."""

        mock_topics = mock.Mock()
        mock_topic = mock.Mock()
        mock_topic.execute.side_effect = errors.HttpError(
            mock.Mock(status=404), 'not found')
        mock_topics.get.return_value = mock_topic
        create_topic = mock.Mock()
        mock_topics.create.return_value = create_topic
        self.mock_pubsub.topics.return_value = mock_topics

        self.client.create_topic('foo')

        mock_topics.get.assert_called_once_with(topic='/topics/project/foo')
        mock_topic.execute.assert_called_once_with()
        mock_topics.create.assert_called_once_with(
            body={'name': '/topics/project/foo'})
        create_topic.execute.assert_called_once_with()

    def test_create_error(self):
        """Ensure that if the topic create fails, an exception is raised."""

        mock_topics = mock.Mock()
        mock_topic = mock.Mock()
        mock_topic.execute.side_effect = errors.HttpError(
            mock.Mock(status=400), 'error')
        mock_topics.get.return_value = mock_topic
        self.mock_pubsub.topics.return_value = mock_topics

        self.assertRaises(errors.HttpError, self.client.create_topic, 'foo')

        mock_topics.get.assert_called_once_with(topic='/topics/project/foo')
        mock_topic.execute.assert_called_once_with()


class TestSubscribe(unittest.TestCase):

    def setUp(self):
        self.project_id = 'project'
        self.mock_pubsub = mock.Mock()
        self.client = client.PubSubClient(self.mock_pubsub, self.project_id)

    def test_subscription_exists(self):
        """Ensure that nothing happens if the subscription already exists."""

        mock_subscriptions = mock.Mock()
        mock_subscription = mock.Mock()
        mock_subscriptions.get.return_value = mock_subscription
        self.mock_pubsub.subscriptions.return_value = mock_subscriptions

        self.client.subscribe('foo', 'bar', 'https://baz.com')

        mock_subscriptions.get.assert_called_once_with(
            subscription='/subscriptions/project/foo')
        mock_subscription.execute.assert_called_once_with()

    def test_subscription_doesnt_exist(self):
        """Ensure that the subscription is created if it doesn't exist."""

        mock_subscriptions = mock.Mock()
        mock_subscription = mock.Mock()
        mock_subscription.execute.side_effect = errors.HttpError(
            mock.Mock(status=404), 'not found')
        mock_subscriptions.get.return_value = mock_subscription
        create_subscription = mock.Mock()
        mock_subscriptions.create.return_value = create_subscription
        self.mock_pubsub.subscriptions.return_value = mock_subscriptions

        self.client.subscribe('foo', 'bar', 'https://baz.com')

        mock_subscriptions.get.assert_called_once_with(
            subscription='/subscriptions/project/foo')
        mock_subscription.execute.assert_called_once_with()
        mock_subscriptions.create.assert_called_once_with(body={
            'name': '/subscriptions/project/foo',
            'topic': '/topics/project/bar',
            'pushConfig': {
                'pushEndpoint': 'https://baz.com',
            }
        })
        create_subscription.execute.assert_called_once_with()

    def test_subscribe_error(self):
        """Ensure that if the subscription create fails, an exception is
        raised.
        """

        mock_subscriptions = mock.Mock()
        mock_subscription = mock.Mock()
        mock_subscription.execute.side_effect = errors.HttpError(
            mock.Mock(status=400), 'error')
        mock_subscriptions.get.return_value = mock_subscription
        self.mock_pubsub.subscriptions.return_value = mock_subscriptions

        self.assertRaises(errors.HttpError, self.client.subscribe, 'foo',
                          'bar', 'https://baz.com')

        mock_subscriptions.get.assert_called_once_with(
            subscription='/subscriptions/project/foo')
        mock_subscription.execute.assert_called_once_with()

