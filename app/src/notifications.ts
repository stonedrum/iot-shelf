import * as Notifications from 'expo-notifications';
import { NativeAppEventEmitter, NativeModules, Platform } from 'react-native';
import Getui from 'react-native-getui';
import { registerPushToken } from './api';

type GetuiAndroidEvent = {
  type?: 'cid' | 'payload' | 'cmd' | 'notificationArrived' | 'notificationClicked';
  cid?: string;
  payload?: string;
};

async function saveCid(cid?: string): Promise<void> {
  const value = cid?.trim();
  if (!value) return;
  await registerPushToken(value, Platform.OS);
}

function readCurrentCid(): void {
  Getui.clientId((cid) => {
    saveCid(cid).catch(error => console.warn('个推 CID 登记失败', error));
  });
}

export async function setupPushNotifications(
  onOrderNotification: () => void,
): Promise<() => void> {
  if (!NativeModules.GetuiModule) {
    console.warn('当前运行环境没有个推原生模块，请使用 development/production build，不支持 Expo Go');
    return () => undefined;
  }

  const subscriptions = [
    NativeAppEventEmitter.addListener(
      'receiveRemoteNotification',
      (event: GetuiAndroidEvent) => {
        if (event.type === 'cid') {
          saveCid(event.cid).catch(error => console.warn('个推 CID 登记失败', error));
        } else if (event.type === 'notificationClicked') {
          onOrderNotification();
        }
      },
    ),
    NativeAppEventEmitter.addListener('GeTuiSdkDidRegisterClient', (cid: string) => {
      saveCid(cid).catch(error => console.warn('个推 CID 登记失败', error));
    }),
    NativeAppEventEmitter.addListener('GeTuiSdkDidReceiveNotification', () => {
      onOrderNotification();
    }),
  ];

  if (Platform.OS === 'android') {
    const permission = await Notifications.getPermissionsAsync();
    if (permission.status !== 'granted') {
      await Notifications.requestPermissionsAsync();
    }
    Getui.initPush();
  }
  Getui.turnOnPush();
  readCurrentCid();

  return () => subscriptions.forEach(subscription => subscription.remove());
}
