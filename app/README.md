# 搬夫送水原生 App

基于 Expo / React Native，同时支持 Android 和 iOS。

## 本地启动

```bash
npm install
cp .env.example .env
npx expo run:android
# 或连接已配置签名的 iPhone 后运行：
npx expo run:ios --device
```

真机访问本地后端时，`.env` 中不能使用 `127.0.0.1`，需要填写电脑的局域网 IP。

## 个推新订单提醒

App 已接入 `react-native-getui`，登录后会获取 CID 并登记到后端。MQTT 或 Web 后端创建新订单后，会通过个推 RestAPI V2 向有权访问订单所在城市的用户推送提醒。

个推是原生模块，不能使用 Expo Go；请使用真机 Development Build 或正式安装包。Android 基础个推通道已启用。若要提高应用被彻底结束后的到达率，还需在个推控制台配置华为、小米、OPPO、vivo、荣耀等厂商通道。

iOS 正式推送还需在个推控制台上传与 `com.banfu.waterdelivery` 对应的 APNs Auth Key（推荐）或推送证书，并使用具有 Push Notifications 权限的签名。

## 页面

- 默认页：待配送订单
- 货架管理：手工生成订单或查看已有订单
- 历史订单：已送达和已取消
- 确认送达：输入实际送水数量，订单完成并增加货架余量
